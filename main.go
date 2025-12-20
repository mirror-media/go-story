package main

import (
	"log"
	"net/http"

	"go-story/internal/config"
	"go-story/internal/data"
	"go-story/internal/schema"
	"go-story/internal/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	db, err := data.NewDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	defer db.Close()

	repo := data.NewRepo(db, cfg.StaticsHost)
	gqlSchema, err := schema.Build(repo)
	if err != nil {
		log.Fatalf("failed to build schema: %v", err)
	}

	http.Handle("/api/graphql", server.NewGraphQLHandler(gqlSchema))
	http.HandleFunc("/probe", server.ProbeHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("GraphQL endpoint is available at POST /api/graphql"))
	})

	addr := ":" + cfg.Port
	log.Printf("GraphQL server listening on %s (POST /api/graphql)", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
