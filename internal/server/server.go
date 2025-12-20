package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/graphql-go/graphql"
)

func NewGraphQLHandler(schema graphql.Schema) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("only POST is supported at /api/graphql"))
			return
		}

		var payload struct {
			Query         string                 `json:"query"`
			Variables     map[string]interface{} `json:"variables"`
			OperationName string                 `json:"operationName"`
		}

		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}

		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  payload.Query,
			VariableValues: payload.Variables,
			OperationName:  payload.OperationName,
			Context:        r.Context(),
		})

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})
}

type ProbeResult struct {
	Name       string          `json:"name"`
	StatusCode int             `json:"statusCode"`
	Body       json.RawMessage `json:"body,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// ProbeHandler runs a set of built-in GQL queries against target URL.
func ProbeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST", http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.URL == "" {
		http.Error(w, "invalid payload, need {\"url\": \"https://your-gql\"}", http.StatusBadRequest)
		return
	}

	results := runProbeTests(payload.URL)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"target":  payload.URL,
		"results": results,
	})
}

func runProbeTests(target string) []ProbeResult {
	client := &http.Client{Timeout: 10 * time.Second}

	tests := []struct {
		name string
		body map[string]any
	}{
		{
			name: "posts_list",
			body: map[string]any{
				"query": `query ($take:Int,$skip:Int,$orderBy:[PostOrderByInput!]!,$filter:PostWhereInput!){
					postsCount(where:$filter)
					posts(take:$take,skip:$skip,orderBy:$orderBy,where:$filter){
						id slug title publishedDate state
					}
				}`,
				"variables": map[string]any{
					"take":    3,
					"skip":    0,
					"orderBy": []map[string]string{{"publishedDate": "desc"}},
					"filter":  map[string]any{},
				},
			},
		},
		{
			name: "post_by_slug",
			body: map[string]any{
				"query": `query ($slug:String){ post(where:{slug:$slug}){ id slug title state } }`,
				"variables": map[string]any{
					"slug": "20251212-4-173036",
				},
			},
		},
		{
			name: "externals_list",
			body: map[string]any{
				"query": `query ($take:Int,$skip:Int,$orderBy:[ExternalOrderByInput!]!,$filter:ExternalWhereInput!){
					externals(take:$take,skip:$skip,orderBy:$orderBy,where:$filter){
						id slug title thumb brief publishedDate partner{ id slug name showOnIndex }
					}
				}`,
				"variables": map[string]any{
					"take":    3,
					"skip":    0,
					"orderBy": []map[string]string{{"publishedDate": "desc"}},
					"filter":  map[string]any{},
				},
			},
		},
		{
			name: "external_by_slug",
			body: map[string]any{
				"query": `query ($slug:String){
					externals(where:{slug:{equals:$slug},state:{equals:"published"}}){
						id slug title thumb brief content publishedDate extend_byline thumbCaption
						partner{ id slug name showOnIndex showThumb showBrief }
						updatedAt
					}
				}`,
				"variables": map[string]any{
					"slug": "mirrordaily_35695",
				},
			},
		},
	}

	results := make([]ProbeResult, 0, len(tests))
	for _, t := range tests {
		res := ProbeResult{Name: t.name}
		b, _ := json.Marshal(t.body)
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(b))
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		res.StatusCode = resp.StatusCode
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			res.Error = err.Error()
		} else {
			res.Body = json.RawMessage(body)
		}
		results = append(results, res)
	}
	return results
}
