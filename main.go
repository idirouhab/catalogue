package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/go-agent/v3/integrations/nrgorilla"
	"github.com/newrelic/go-agent/v3/integrations/nrlogrus"
	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/newrelic/go-agent/v3/integrations/nrmysql"
	"github.com/newrelic/go-agent/v3/newrelic"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type listRequest struct {
	Tags     []string `json:"tags"`
	Order    string   `json:"order"`
	PageNum  int      `json:"pageNum"`
	PageSize int      `json:"pageSize"`
}

// Sock describes the thing on offer in the catalogue.
type Sock struct {
	ID          string   `json:"id" db:"id"`
	Name        string   `json:"name" db:"name"`
	Description string   `json:"description" db:"description"`
	ImageURL    []string `json:"imageUrl" db:"-"`
	ImageURL_1  string   `json:"-" db:"image_url_1"`
	ImageURL_2  string   `json:"-" db:"image_url_2"`
	Price       float32  `json:"price" db:"price"`
	Count       int      `json:"count" db:"count"`
	Tags        []string `json:"tag" db:"-"`
	TagString   string   `json:"-" db:"tag_name"`
}

type countResponse struct {
	N   int   `json:"size"` // to match original
	Err error `json:"err"`
}

// Health describes the health of a service
type Health struct {
	Service string `json:"service"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

var Socks []Sock

var baseQuery = "SELECT sock.sock_id AS id, sock.name, sock.description, sock.price, sock.count, sock.image_url_1, sock.image_url_2, GROUP_CONCAT(tag.name) AS tag_name FROM sock JOIN sock_tag ON sock.sock_id=sock_tag.sock_id JOIN tag ON sock_tag.tag_id=tag.tag_id"

// ErrNotFound is returned when there is no sock for a given ID.
func main() {
	app, err := newrelic.NewApplication(
		newrelic.ConfigDistributedTracerEnabled(true),
		nrlogrus.ConfigStandardLogger(),
		func(cfg *newrelic.Config) {
			cfg.ErrorCollector.RecordPanics = true
		},
		newrelic.ConfigAppName(os.Getenv("NEW_RELIC_APP_NAME")),
		newrelic.ConfigLicense(os.Getenv("NEW_RELIC_LICENSE_KEY")),
	)

	if nil != err {
		fmt.Println(err)
		os.Exit(1)
	}

	router := mux.NewRouter()
	router.HandleFunc("/catalogue", getCatalogue).Methods("GET")
	router.HandleFunc("/catalogue/size", count).Methods("GET")
	router.HandleFunc("/catalogue/{id}", getSock).Methods("GET")
	router.HandleFunc("/tags", getTags).Methods("GET")
	router.HandleFunc("/health", health).Methods("GET")
	router.Methods("GET").PathPrefix("/catalogue/images/").Handler(http.StripPrefix(
		"/catalogue/images/",
		http.FileServer(http.Dir("./images/")),
	))

	router.Use(customMiddleware)
	http.ListenAndServe(":80", nrgorilla.InstrumentRoutes(router, app))

}

var decoder = schema.NewDecoder()

func health(w http.ResponseWriter, r *http.Request) {
	db, err := sqlx.Open("mysql", "catalogue_user:default_password@tcp(catalogue-db:3306)/socksdb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	var health []Health
	dbstatus := "OK"

	err = db.Ping()
	if err != nil {
		dbstatus = "err"
	}

	app := Health{"catalogue", "OK", time.Now().String()}
	dbS := Health{"catalogue-db", dbstatus, time.Now().String()}

	health = append(health, app)
	health = append(health, dbS)

	json.NewEncoder(w).Encode(health)

}

func getTags(w http.ResponseWriter, r *http.Request) {
	db, err := sqlx.Open("mysql", "catalogue_user:default_password@tcp(catalogue-db:3306)/socksdb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	var tags []string
	query := "SELECT name FROM tag;"
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	var tag string
	for rows.Next() {
		err = rows.Scan(&tag)
		if err != nil {
			continue
		}
		tags = append(tags, tag)
	}

	json.NewEncoder(w).Encode(tags)
}

func getCatalogue(w http.ResponseWriter, r *http.Request) {
	var socks []Sock
	query := baseQuery
	db, err := sqlx.Open("mysql", "catalogue_user:default_password@tcp(catalogue-db:3306)/socksdb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	var args []interface{}
	tags := r.URL.Query().Get("tags")
	for i, t := range strings.Split(tags, ",") {
		if len(tags) > 0 {
			if i == 0 {
				query += " WHERE tag.name=?"
				args = append(args, t)
			} else {
				query += " OR tag.name=?"
				args = append(args, t)
			}
		}
	}

	query += " GROUP BY id"

	order := r.URL.Query().Get("order")
	if order != "" {
		query += " ORDER BY ?"
		args = append(args, order)
	}

	query += ";"

	err = db.Select(&socks, query, args...)
	if err != nil {
		panic(err)
	}
	for i, s := range socks {
		socks[i].ImageURL = []string{s.ImageURL_1, s.ImageURL_2}
		socks[i].Tags = strings.Split(s.TagString, ",")
	}

	pageNum := 1
	if page := r.URL.Query().Get("pageNum"); page != "" {
		pageNum, _ = strconv.Atoi(page)
	}
	pageSize := 10
	if size := r.URL.Query().Get("pageSize"); size != "" {
		pageSize, _ = strconv.Atoi(size)
	}

	socks = cut(socks, pageNum, pageSize)

	json.NewEncoder(w).Encode(socks)
}

func count(w http.ResponseWriter, r *http.Request) {
	db, err := sqlx.Open("mysql", "catalogue_user:default_password@tcp(catalogue-db:3306)/socksdb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	query := "SELECT COUNT(DISTINCT sock.sock_id) FROM sock JOIN sock_tag ON sock.sock_id=sock_tag.sock_id JOIN tag ON sock_tag.tag_id=tag.tag_id"
	var args []interface{}

	tags := r.URL.Query().Get("tags")
	for i, t := range strings.Split(tags, ",") {
		if len(tags) > 0 {
			if i == 0 {
				query += " WHERE tag.name=?"
				args = append(args, t)
			} else {
				query += " OR tag.name=?"
				args = append(args, t)
			}
		}
	}

	query += ";"
	sel, err := db.Prepare(query)

	if err != nil {
		panic(err)
	}
	defer sel.Close()

	var count int
	err = sel.QueryRow(args...).Scan(&count)
	var countResponse countResponse
	if err != nil {
		panic(err)
	}
	countResponse.N = count

	json.NewEncoder(w).Encode(countResponse)
}

func getSock(w http.ResponseWriter, r *http.Request) {
	db, err := sqlx.Open("mysql", "catalogue_user:default_password@tcp(catalogue-db:3306)/socksdb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	query := baseQuery + " WHERE sock.sock_id =? GROUP BY sock.sock_id;"

	var sock Sock
	err = db.Get(&sock, query, mux.Vars(r)["id"])
	if err != nil {
		panic(err)
	}

	sock.ImageURL = []string{sock.ImageURL_1, sock.ImageURL_2}
	sock.Tags = strings.Split(sock.TagString, ",")

	json.NewEncoder(w).Encode(sock)
}

func customMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txn := newrelic.FromContext(r.Context())
		segment := txn.StartSegment("customMiddleware")
		segment.End()

		next.ServeHTTP(w, r)
	})
}

func cut(socks []Sock, pageNum, pageSize int) []Sock {
	if pageNum == 0 || pageSize == 0 {
		return []Sock{} // pageNum is 1-indexed
	}
	start := (pageNum * pageSize) - pageSize
	if start > len(socks) {
		return []Sock{}
	}
	end := pageNum * pageSize
	if end > len(socks) {
		end = len(socks)
	}
	return socks[start:end]
}
