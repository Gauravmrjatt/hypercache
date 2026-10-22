package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Gauravmrjatt/hypercache/internal/metrics"
	"github.com/Gauravmrjatt/hypercache/internal/protocol"
)

// ErrServerClosed is returned by Serve after Shutdown has been called.
var ErrServerClosed = errors.New("server closed")

const (
	readBufferSize  = 64 * 1024
	writeBufferSize = 64 * 1024
)

// Config configures the TCP server.
type Config struct {
	// Addr is the listen address, for example "127.0.0.1:6380".
	Addr string
	// ReadTimeout closes connections that are idle for longer than this.
	// Zero disables the timeout.
	ReadTimeout time.Duration
}

// Server accepts client connections and executes commands against a cache.
type Server struct {
	cfg     Config
	handler *Handler
	metrics *metrics.Collector

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// New creates a server that is not yet listening.
func New(cfg Config, handler *Handler, m *metrics.Collector) *Server {
	return &Server{
		cfg:     cfg,
		handler: handler,
		metrics: m,
		conns:   make(map[net.Conn]struct{}),
	}
}

// Listen opens the configured listener.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	return nil
}

// Addr returns the bound address, or nil before Listen succeeds.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// ListenAndServe listens on the configured address and serves until Shutdown
// is called. It returns ErrServerClosed after a graceful shutdown.
func (s *Server) ListenAndServe() error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve(s.ln)
}

// Serve accepts connections on ln until Shutdown is called.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrServerClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return ErrServerClosed
			}
			continue
		}
		s.wg.Add(1)
		s.metrics.ConnOpened()
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(conn)
	}
}

// Shutdown stops accepting connections and waits for in-flight handlers
// until ctx expires.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	// Idle connections block in Read; closing them releases the handlers.
	for _, conn := range conns {
		_ = conn.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close is a convenience wrapper around Shutdown with a short deadline.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.metrics.ConnClosed()
		s.wg.Done()
	}()

	reader := bufio.NewReaderSize(conn, readBufferSize)
	writer := protocol.NewWriter(bufio.NewWriterSize(conn, writeBufferSize))

	for {
		if s.cfg.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && strings.TrimSpace(line) != "" {
				s.dispatch(line, writer)
				_ = writer.Flush()
			}
			return
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		start := time.Now()
		s.dispatch(line, writer)
		if flushErr := writer.Flush(); flushErr != nil {
			return
		}
		s.metrics.ObserveLatency(time.Since(start))
	}
}

// dispatch parses and executes a single command line.
func (s *Server) dispatch(line string, w *protocol.Writer) {
	cmd, err := protocol.Parse(line)
	if err != nil {
		s.metrics.IncParseError()
		_ = w.Error(err.Error())
		return
	}
	s.metrics.IncCommand(cmd.Name)
	_ = s.handler.Handle(cmd, w)
}
