package server

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// GracefulServer wraps http.Server with graceful shutdown on SIGTERM/SIGINT.
type GracefulServer struct {
	srv     *http.Server
	onStop  func()
}

// NewGracefulServer creates a server that shuts down gracefully on OS signals.
// onStop is called before shutdown begins (e.g. to persist state).
func NewGracefulServer(addr string, handler http.Handler, onStop func()) *GracefulServer {
	return &GracefulServer{
		srv: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 120 * time.Second, // long enough for streaming
			IdleTimeout:  60 * time.Second,
		},
		onStop: onStop,
	}
}

// ListenAndServe starts the server and blocks until a shutdown signal arrives.
// It then drains active connections for up to 30 seconds before exiting.
func (gs *GracefulServer) ListenAndServe() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("gateway: listening on %s", gs.srv.Addr)
		errCh <- gs.srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		log.Printf("gateway: received %s, shutting down gracefully", sig)
	}

	// Stop accepting new requests immediately.
	if gs.onStop != nil {
		gs.onStop()
	}

	// Drain active connections with a 30-second deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := gs.srv.Shutdown(ctx); err != nil {
		log.Printf("gateway: shutdown error: %v", err)
		return err
	}
	log.Println("gateway: shutdown complete")
	return nil
}

// ListenAndServeTLS is the TLS variant of ListenAndServe.
func (gs *GracefulServer) ListenAndServeTLS(certFile, keyFile string) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("gateway: listening on %s (TLS)", gs.srv.Addr)
		errCh <- gs.srv.ListenAndServeTLS(certFile, keyFile)
	}()

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		log.Printf("gateway: received %s, shutting down gracefully", sig)
	}

	if gs.onStop != nil {
		gs.onStop()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := gs.srv.Shutdown(ctx); err != nil {
		log.Printf("gateway: shutdown error: %v", err)
		return err
	}
	log.Println("gateway: shutdown complete")
	return nil
}
