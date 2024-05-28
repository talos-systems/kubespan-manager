// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

// Package main implements the discovery service entrypoint.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/jonboulle/clockwork"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siderolabs/discovery-api/api/v1alpha1/server/pb"
	"github.com/siderolabs/go-debug"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/siderolabs/discovery-service/internal/landing"
	"github.com/siderolabs/discovery-service/internal/limiter"
	_ "github.com/siderolabs/discovery-service/internal/proto"
	"github.com/siderolabs/discovery-service/internal/state/storage"
	"github.com/siderolabs/discovery-service/pkg/limits"
	"github.com/siderolabs/discovery-service/pkg/server"
	"github.com/siderolabs/discovery-service/pkg/state"
)

var (
	listenAddr       = ":3000"
	landingAddr      = ":3001"
	metricsAddr      = ":2122"
	debugAddr        = ":2123"
	devMode          = false
	gcInterval       = time.Minute
	redirectEndpoint = ""
	snapshotsEnabled = true
	snapshotPath     = "/var/discovery-service/state.binpb"
	snapshotInterval = 10 * time.Minute
)

func init() {
	flag.StringVar(&listenAddr, "addr", listenAddr, "addr on which to listen")
	flag.StringVar(&landingAddr, "landing-addr", landingAddr, "addr on which to listen for landing page")
	flag.StringVar(&metricsAddr, "metrics-addr", metricsAddr, "prometheus metrics listen addr")
	flag.BoolVar(&devMode, "debug", devMode, "enable debug mode")
	flag.DurationVar(&gcInterval, "gc-interval", gcInterval, "garbage collection interval")
	flag.StringVar(&redirectEndpoint, "redirect-endpoint", redirectEndpoint, "redirect all clients to a new endpoint (gRPC endpoint, e.g. 'example.com:443'")
	flag.BoolVar(&snapshotsEnabled, "snapshots-enabled", snapshotsEnabled, "enable snapshots")
	flag.StringVar(&snapshotPath, "snapshot-path", snapshotPath, "path to the snapshot file")
	flag.DurationVar(&snapshotInterval, "snapshot-interval", snapshotInterval, "interval to save the snapshot")

	if debug.Enabled {
		flag.StringVar(&debugAddr, "debug-addr", debugAddr, "debug (pprof, trace, expvar) listen addr")
	}
}

func main() {
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalln("failed to initialize logger:", err)
	}

	if os.Getenv("MODE") == "dev" {
		devMode = true
	}

	if devMode {
		logger, err = zap.NewDevelopment()
		if err != nil {
			log.Fatalln("failed to initialize development logger:", err)
		}
	}

	zap.ReplaceGlobals(logger)
	zap.RedirectStdLog(logger)

	if err = signalHandler(context.Background(), logger, run); err != nil {
		logger.Error("service failed", zap.Error(err))

		os.Exit(1)
	}
}

func signalHandler(ctx context.Context, logger *zap.Logger, f func(ctx context.Context, logger *zap.Logger) error) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	return f(ctx, logger)
}

func recoveryHandler(logger *zap.Logger) grpc_recovery.RecoveryHandlerFunc {
	return func(p interface{}) error {
		if logger != nil {
			logger.Error("grpc panic", zap.Any("panic", p), zap.Stack("stack"))
		}

		return status.Errorf(codes.Internal, "%v", p)
	}
}

func interceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(_ context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)

		for i := 0; i < len(fields); i += 2 {
			key := fields[i].(string) //nolint:forcetypeassert,errcheck
			value := fields[i+1]

			switch v := value.(type) {
			case string:
				f = append(f, zap.String(key, v))
			case int:
				f = append(f, zap.Int(key, v))
			case bool:
				f = append(f, zap.Bool(key, v))
			default:
				f = append(f, zap.Any(key, v))
			}
		}

		logger := l.WithOptions(zap.AddCallerSkip(1)).With(f...)

		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg)
		case logging.LevelInfo:
			logger.Info(msg)
		case logging.LevelWarn:
			logger.Warn(msg)
		case logging.LevelError:
			logger.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}

func run(ctx context.Context, logger *zap.Logger) error {
	logger.Info("service starting")

	defer logger.Info("service shut down")

	recoveryOpt := grpc_recovery.WithRecoveryHandler(recoveryHandler(logger))

	limiter := limiter.NewIPRateLimiter(limits.IPRateRequestsPerSecondMax, limits.IPRateBurstSizeMax)

	metrics := grpc_prometheus.NewServerMetrics(
		grpc_prometheus.WithServerHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets([]float64{0.01, 0.1, 0.25, 0.5, 1.0, 2.5})),
	)

	loggingOpts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
		logging.WithFieldsFromContext(logging.ExtractFields),
	}

	//nolint:contextcheck
	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			server.AddLoggingFieldsUnaryServerInterceptor(),
			logging.UnaryServerInterceptor(interceptorLogger(logger), loggingOpts...),
			server.RateLimitUnaryServerInterceptor(limiter),
			metrics.UnaryServerInterceptor(),
			grpc_recovery.UnaryServerInterceptor(recoveryOpt),
		),
		grpc.ChainStreamInterceptor(
			server.AddLoggingFieldsStreamServerInterceptor(),
			server.RateLimitStreamServerInterceptor(limiter),
			logging.StreamServerInterceptor(interceptorLogger(logger), loggingOpts...),
			metrics.StreamServerInterceptor(),
			grpc_recovery.StreamServerInterceptor(recoveryOpt),
		),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: 10 * time.Second,
		}),
		grpc.SharedWriteBuffer(true),
		experimental.RecvBufferPool(grpc.NewSharedBufferPool()),
		grpc.ReadBufferSize(16 * 1024),
		grpc.WriteBufferSize(16 * 1024),
	}

	state := state.NewState(logger)
	prom.MustRegister(state)

	var stateStorage *storage.Storage

	if snapshotsEnabled {
		stateStorage = storage.New(snapshotPath, state, logger)
		prom.MustRegister(stateStorage)

		if err := stateStorage.Load(); err != nil {
			logger.Warn("failed to load state from storage", zap.Error(err))
		}
	} else {
		logger.Info("snapshots are disabled")
	}

	srv := server.NewClusterServer(state, ctx.Done(), redirectEndpoint)
	prom.MustRegister(srv)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	landingLis, err := net.Listen("tcp", landingAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer(serverOptions...)
	pb.RegisterClusterServer(s, srv)

	metrics.InitializeMetrics(s)

	if err = prom.Register(metrics); err != nil {
		return fmt.Errorf("failed to register metrics: %w", err)
	}

	var metricsMux http.ServeMux

	metricsMux.Handle("/metrics", promhttp.Handler())

	metricsServer := http.Server{
		Addr:    metricsAddr,
		Handler: &metricsMux,
	}

	landingServer := http.Server{
		Handler: landing.Handler(state, logger),
	}

	eg, ctx := errgroup.WithContext(ctx)

	if snapshotsEnabled {
		eg.Go(func() error {
			return stateStorage.Start(ctx, clockwork.NewRealClock(), snapshotInterval)
		})
	}

	eg.Go(func() error {
		logger.Info("gRPC server starting", zap.Stringer("address", lis.Addr()))

		if err := s.Serve(lis); err != nil {
			return fmt.Errorf("failed to serve: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		logger.Info("landing server starting", zap.Stringer("address", landingLis.Addr()))

		if err := landingServer.Serve(landingLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("failed to serve: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		logger.Info("metrics starting", zap.String("address", metricsServer.Addr))

		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	})

	eg.Go(func() error {
		<-ctx.Done()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		s.GracefulStop()
		landingServer.Shutdown(ctx)         //nolint:errcheck
		metricsServer.Shutdown(shutdownCtx) //nolint:errcheck,contextcheck

		return nil
	})

	eg.Go(func() error {
		state.RunGC(ctx, logger, gcInterval)

		return nil
	})

	eg.Go(func() error {
		limiter.RunGC(ctx)

		return nil
	})

	eg.Go(func() error {
		return debug.ListenAndServe(ctx, debugAddr, func(msg string) { logger.Info(msg) })
	})

	return eg.Wait()
}
