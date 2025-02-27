package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/sessions"
	datastar "github.com/starfederation/datastar/sdk/go"
	"golang.org/x/sync/errgroup"
)

func setupCounterRoute(router chi.Router, sessionStore sessions.Store) error {
	const (
		sessionKey = "counter"
		countKey   = "count"
	)

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		CounterInitial().Render(r.Context(), w)
	})

	var globalCounter atomic.Uint32

	GetUserValue := func(r *http.Request) (uint32, *sessions.Session, error) {
		session, err := sessionStore.Get(r, sessionKey)
		if err != nil {
			return 0, nil, err
		}

		val, ok := session.Values[countKey].(uint32)
		if !ok {
			val = 0
		}

		return val, session, nil
	}

	router.Get("/counter/data", func(w http.ResponseWriter, r *http.Request) {
		userCount, _, err := GetUserValue(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		store := CounterSignals{
			Global: globalCounter.Load(),
			User:   userCount,
		}

		datastar.NewSSE(w, r).MergeFragmentTempl(Counter(store))
	})

	updateGlobal := func(store *gabs.Container) {
		store.Set(globalCounter.Add(1), "global")
	}

	router.Route("/counter/increment", func(incrementRouter chi.Router) {
		incrementRouter.Post("/global", func(w http.ResponseWriter, r *http.Request) {
			update := gabs.New()
			updateGlobal(update)

			datastar.NewSSE(w, r).MarshalAndMergeSignals(update)
		})

		incrementRouter.Post("/user", func(w http.ResponseWriter, r *http.Request) {
			val, sess, err := GetUserValue(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			val++
			sess.Values[countKey] = val
			if err := sess.Save(r, w); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			update := gabs.New()
			updateGlobal(update)
			update.Set(val, "user")

			datastar.NewSSE(w, r).MarshalAndMergeSignals(update)
		})
	})

	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	getPort := func() string {
		if p, ok := os.LookupEnv("PORT"); ok {
			return p
		}
		return "8080"
	}
	logger.Info("Starting Server 0.0.0.0:" + getPort())
	defer logger.Info("Stopping Server")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, getPort()); err != nil {
		logger.Error("Error running server", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, port string) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(startServer(ctx, port))

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error running server: %w", err)
	}

	return nil
}

func startServer(ctx context.Context, port string) func() error {
	return func() error {
		router := chi.NewMux()

		router.Use(
			middleware.Logger,
			middleware.Recoverer,
		)

		router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

		sessionStore := sessions.NewCookieStore([]byte("session-secret"))
		sessionStore.Options = &sessions.Options{
			Domain:   "localhost",
			MaxAge:   int(24 * time.Hour / time.Second),
			SameSite: http.SameSiteLaxMode,
		}

		if err := setupCounterRoute(router, sessionStore); err != nil {
			return fmt.Errorf("error setting up routes: %w", err)
		}

		srv := &http.Server{
			Addr:    "0.0.0.0:" + port,
			Handler: router,
		}

		go func() {
			<-ctx.Done()
			srv.Shutdown(context.Background())
		}()

		return srv.ListenAndServe()
	}
}
