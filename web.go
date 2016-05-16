package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

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

func startWeb(s Store, bind string) {
	h := &pdnsHandler{s: s}
	r := mux.NewRouter()
	r.HandleFunc("/dns/{searchType}/tuples/{query}", h.handleSearchTuples)
	r.HandleFunc("/dns/{searchType}/individual/{query}", h.handleSearchIndividual)
	http.Handle("/", r)

	log.Printf("Listening on %q\n", bind)
	log.Fatal(http.ListenAndServe(bind, nil))
}
