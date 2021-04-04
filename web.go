package main

import (
	"embed"
	"encoding/json"
	"log"
	"net/http"
	"text/template"

	"github.com/gorilla/mux"
)

//go:embed template
var content embed.FS

type pdnsHandler struct {
	s Store
}

func (h *pdnsHandler) handleSearchTuples(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	searchType := vars["searchType"]
	query := vars["query"]

	if query == "" {
		http.Error(w, "Missing parameter: q", http.StatusBadRequest)
		return
	}
	var err error
	var recs tupleResults
	if searchType == "like" {
		recs, err = h.s.LikeTuples(query)
	} else {
		recs, err = h.s.FindTuples(query)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(recs)
}
func (h *pdnsHandler) handleSearchIndividual(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	searchType := vars["searchType"]
	query := vars["query"]

	if query == "" {
		http.Error(w, "Missing parameter: q", http.StatusBadRequest)
		return
	}
	var err error
	var recs individualResults
	if searchType == "like" {
		recs, err = h.s.LikeIndividual(query)
	} else {
		recs, err = h.s.FindIndividual(query)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(recs)
}

type Results struct {
	Query      string
	Exact      bool
	Individual individualResults
	Tuples     tupleResults
	Error      error
}

func (h *pdnsHandler) handleUI(w http.ResponseWriter, req *http.Request) {
	var err error
	var res Results
	res.Query = req.FormValue("query")
	res.Exact = req.FormValue("exact") == "on"
	if res.Query != "" {
		if res.Exact {
			res.Individual, err = h.s.FindIndividual(res.Query)
			if err != nil {
				res.Error = err
			}
			res.Tuples, err = h.s.FindTuples(res.Query)
			if err != nil {
				res.Error = err
			}
		} else {
			res.Individual, err = h.s.LikeIndividual(res.Query)
			if err != nil {
				res.Error = err
			}
			res.Tuples, err = h.s.LikeTuples(res.Query)
			if err != nil {
				res.Error = err
			}
		}
	}

	var t = template.Must(template.New("index.html").ParseFS(content, "template/index.html"))
	err = t.Execute(w, res)
	if err != nil {
		log.Printf("Error rendering template: %v", err)
	}
}

func startWeb(s Store, bind string) {
	h := &pdnsHandler{s: s}
	r := mux.NewRouter()

	r.HandleFunc("/dns/{searchType}/tuples/{query}", h.handleSearchTuples)
	r.HandleFunc("/dns/{searchType}/individual/{query}", h.handleSearchIndividual)

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusSeeOther)
	})
	r.HandleFunc("/ui/", h.handleUI)

	http.Handle("/", r)

	log.Printf("Listening on %q\n", bind)
	log.Fatal(http.ListenAndServe(bind, nil))
}
