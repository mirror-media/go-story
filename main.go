package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mitchellh/mapstructure"
)

type imageFile struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type resized struct {
	Original string `json:"original"`
	W480     string `json:"w480"`
	W800     string `json:"w800"`
	W1200    string `json:"w1200"`
	W1600    string `json:"w1600"`
	W2400    string `json:"w2400"`
}

type photo struct {
	ID          string         `json:"id"`
	ImageFile   imageFile      `json:"imageFile"`
	Resized     resized        `json:"resized"`
	ResizedWebp resized        `json:"resizedWebp"`
	Metadata    map[string]any `json:"-"`
}

type section struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Slug  string `json:"slug"`
	State string `json:"state"`
}

type category struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Slug         string    `json:"slug"`
	State        string    `json:"state"`
	IsMemberOnly bool      `json:"isMemberOnly"`
	Sections     []section `json:"sections"`
}

type contact struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type tag struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type video struct {
	ID        string `json:"id"`
	VideoSrc  string `json:"videoSrc"`
	HeroImage *photo `json:"heroImage"`
}

type partner struct {
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	ShowOnIndex bool   `json:"showOnIndex"`
	ShowThumb   bool   `json:"showThumb"`
	ShowBrief   bool   `json:"showBrief"`
}

type topic struct {
	Slug string `json:"slug"`
}

type post struct {
	ID                     string         `json:"id"`
	Slug                   string         `json:"slug"`
	Title                  string         `json:"title"`
	Subtitle               string         `json:"subtitle"`
	State                  string         `json:"state"`
	Style                  string         `json:"style"`
	PublishedDate          string         `json:"publishedDate"`
	UpdatedAt              string         `json:"updatedAt"`
	IsMember               bool           `json:"isMember"`
	IsAdult                bool           `json:"isAdult"`
	Sections               []section      `json:"sections"`
	SectionsInInputOrder   []section      `json:"sectionsInInputOrder"`
	Categories             []category     `json:"categories"`
	CategoriesInInputOrder []category     `json:"categoriesInInputOrder"`
	Writers                []contact      `json:"writers"`
	WritersInInputOrder    []contact      `json:"writersInInputOrder"`
	Photographers          []contact      `json:"photographers"`
	CameraMan              []contact      `json:"camera_man"`
	Designers              []contact      `json:"designers"`
	Engineers              []contact      `json:"engineers"`
	Vocals                 []contact      `json:"vocals"`
	ExtendByline           string         `json:"extend_byline"`
	Tags                   []tag          `json:"tags"`
	TagsAlgo               []tag          `json:"tags_algo"`
	HeroVideo              *video         `json:"heroVideo"`
	HeroImage              *photo         `json:"heroImage"`
	HeroCaption            string         `json:"heroCaption"`
	Brief                  map[string]any `json:"brief"`
	TrimmedContent         map[string]any `json:"trimmedContent"`
	Content                map[string]any `json:"content"`
	Relateds               []post         `json:"relateds"`
	RelatedsInInputOrder   []post         `json:"relatedsInInputOrder"`
	RelatedsOne            *post          `json:"relatedsOne"`
	RelatedsTwo            *post          `json:"relatedsTwo"`
	Redirect               string         `json:"redirect"`
	OgTitle                string         `json:"og_title"`
	OgImage                *photo         `json:"og_image"`
	OgDescription          string         `json:"og_description"`
	HiddenAdvertised       bool           `json:"hiddenAdvertised"`
	IsAdvertised           bool           `json:"isAdvertised"`
	IsFeatured             bool           `json:"isFeatured"`
	Topics                 *topic         `json:"topics"`
	Metadata               map[string]any `json:"-"`
}

type external struct {
	ID            string         `json:"id"`
	Slug          string         `json:"slug"`
	Partner       *partner       `json:"partner"`
	Title         string         `json:"title"`
	State         string         `json:"state"`
	PublishedDate string         `json:"publishedDate"`
	ExtendByline  string         `json:"extend_byline"`
	Thumb         string         `json:"thumb"`
	ThumbCaption  string         `json:"thumbCaption"`
	Brief         string         `json:"brief"`
	Content       string         `json:"content"`
	UpdatedAt     string         `json:"updatedAt"`
	Tags          []tag          `json:"tags"`
	Relateds      []post         `json:"relateds"`
	Metadata      map[string]any `json:"metadata"`
}

type stringFilter struct {
	Equals *string       `mapstructure:"equals"`
	In     []string      `mapstructure:"in"`
	Not    *stringFilter `mapstructure:"not"`
}

type booleanFilter struct {
	Equals *bool `mapstructure:"equals"`
}

type sectionWhereInput struct {
	Slug  *stringFilter `mapstructure:"slug"`
	State *stringFilter `mapstructure:"state"`
}

type sectionManyRelationFilter struct {
	Some *sectionWhereInput `mapstructure:"some"`
}

type categoryWhereInput struct {
	Slug         *stringFilter  `mapstructure:"slug"`
	State        *stringFilter  `mapstructure:"state"`
	IsMemberOnly *booleanFilter `mapstructure:"isMemberOnly"`
}

type categoryManyRelationFilter struct {
	Some *categoryWhereInput `mapstructure:"some"`
}

type partnerWhereInput struct {
	Slug *stringFilter `mapstructure:"slug"`
}

type postWhereInput struct {
	Slug       *stringFilter               `mapstructure:"slug"`
	Sections   *sectionManyRelationFilter  `mapstructure:"sections"`
	Categories *categoryManyRelationFilter `mapstructure:"categories"`
	State      *stringFilter               `mapstructure:"state"`
	IsAdult    *booleanFilter              `mapstructure:"isAdult"`
	IsMember   *booleanFilter              `mapstructure:"isMember"`
}

type postWhereUniqueInput struct {
	ID   *string `mapstructure:"id"`
	Slug *string `mapstructure:"slug"`
}

type externalWhereInput struct {
	Slug    *stringFilter      `mapstructure:"slug"`
	State   *stringFilter      `mapstructure:"state"`
	Partner *partnerWhereInput `mapstructure:"partner"`
}

type orderRule struct {
	Field     string
	Direction string
}

type dataStore struct {
	Posts     []post
	Externals []external
}

var store = buildSampleData()
var db *sql.DB
var staticsHost string

func main() {
	var err error
	staticsHost = os.Getenv("STATICS_HOST")
	if staticsHost == "" {
		log.Fatal("STATICS_HOST not set")
	}
	db, err = connectDB()
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	defer db.Close()

	schema, err := buildSchema()
	if err != nil {
		log.Fatalf("failed to build schema: %v", err)
	}

	http.Handle("/query", newGraphQLHandler(schema))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("GraphQL endpoint is available at POST /query"))
	})
	log.Println("GraphQL server listening on :8080 (POST /query)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func buildSchema() (graphql.Schema, error) {
	jsonScalar := newJSONScalar()
	dateTimeScalar := newDateTimeScalar()

	stringFilterFields := graphql.InputObjectConfigFieldMap{}
	stringFilterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "StringFilter",
		Fields: stringFilterFields,
	})
	stringFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: graphql.String}
	stringFilterFields["in"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.String)}
	stringFilterFields["not"] = &graphql.InputObjectFieldConfig{Type: stringFilterInput}

	booleanFilterFields := graphql.InputObjectConfigFieldMap{}
	booleanFilterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "BooleanFilter",
		Fields: booleanFilterFields,
	})
	booleanFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: graphql.Boolean}

	sectionWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SectionWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":  &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})
	sectionManyRelationFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SectionManyRelationFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: sectionWhereInputType},
		},
	})

	categoryWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "CategoryWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":         &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state":        &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"isMemberOnly": &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
		},
	})
	categoryManyRelationFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "CategoryManyRelationFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: categoryWhereInputType},
		},
	})

	partnerWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PartnerWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})

	postWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":       &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"sections":   &graphql.InputObjectFieldConfig{Type: sectionManyRelationFilterType},
			"categories": &graphql.InputObjectFieldConfig{Type: categoryManyRelationFilterType},
			"state":      &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"isAdult":    &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
			"isMember":   &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
		},
	})

	postWhereUniqueInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostWhereUniqueInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"id":   &graphql.InputObjectFieldConfig{Type: graphql.ID},
			"slug": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	externalWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ExternalWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":    &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state":   &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"partner": &graphql.InputObjectFieldConfig{Type: partnerWhereInputType},
		},
	})

	orderDirectionEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderDirection",
		Values: graphql.EnumValueConfigMap{
			"asc":  &graphql.EnumValueConfig{Value: "asc"},
			"desc": &graphql.EnumValueConfig{Value: "desc"},
		},
	})

	postOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt":     &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"title":         &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	externalOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ExternalOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt":     &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	imageFileType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ImageFile",
		Fields: graphql.Fields{
			"width":  &graphql.Field{Type: graphql.Int},
			"height": &graphql.Field{Type: graphql.Int},
		},
	})

	resizedType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Resized",
		Fields: graphql.Fields{
			"original": &graphql.Field{Type: graphql.String},
			"w480":     &graphql.Field{Type: graphql.String},
			"w800":     &graphql.Field{Type: graphql.String},
			"w1200":    &graphql.Field{Type: graphql.String},
			"w1600":    &graphql.Field{Type: graphql.String},
			"w2400":    &graphql.Field{Type: graphql.String},
		},
	})

	sectionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Section",
		Fields: graphql.Fields{
			"id":    &graphql.Field{Type: graphql.ID},
			"name":  &graphql.Field{Type: graphql.String},
			"slug":  &graphql.Field{Type: graphql.String},
			"state": &graphql.Field{Type: graphql.String},
		},
	})

	categoryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Category",
		Fields: graphql.Fields{
			"id":           &graphql.Field{Type: graphql.ID},
			"name":         &graphql.Field{Type: graphql.String},
			"slug":         &graphql.Field{Type: graphql.String},
			"state":        &graphql.Field{Type: graphql.String},
			"isMemberOnly": &graphql.Field{Type: graphql.Boolean},
			"sections": &graphql.Field{
				Type: graphql.NewList(sectionType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					c, ok := p.Source.(category)
					if !ok {
						return nil, nil
					}
					where, err := decodeSectionWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterSections(c.Sections, where), nil
				},
			},
		},
	})

	contactType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Contact",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{Type: graphql.String},
		},
	})

	tagType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Tag",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{Type: graphql.String},
			"slug": &graphql.Field{Type: graphql.String},
		},
	})

	photoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Photo",
		Fields: graphql.Fields{
			"id":          &graphql.Field{Type: graphql.ID},
			"imageFile":   &graphql.Field{Type: imageFileType},
			"resized":     &graphql.Field{Type: resizedType},
			"resizedWebp": &graphql.Field{Type: resizedType},
		},
	})

	videoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Video",
		Fields: graphql.Fields{
			"id":       &graphql.Field{Type: graphql.ID},
			"videoSrc": &graphql.Field{Type: graphql.String},
			"heroImage": &graphql.Field{
				Type: photoType,
			},
		},
	})

	partnerType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Partner",
		Fields: graphql.Fields{
			"id":          &graphql.Field{Type: graphql.ID},
			"slug":        &graphql.Field{Type: graphql.String},
			"name":        &graphql.Field{Type: graphql.String},
			"showOnIndex": &graphql.Field{Type: graphql.Boolean},
			"showThumb":   &graphql.Field{Type: graphql.Boolean},
			"showBrief":   &graphql.Field{Type: graphql.Boolean},
		},
	})

	topicType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Topic",
		Fields: graphql.Fields{
			"slug": &graphql.Field{Type: graphql.String},
		},
	})

	var postType *graphql.Object
	postType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Post",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":            &graphql.Field{Type: graphql.ID},
				"slug":          &graphql.Field{Type: graphql.String},
				"title":         &graphql.Field{Type: graphql.String},
				"subtitle":      &graphql.Field{Type: graphql.String},
				"state":         &graphql.Field{Type: graphql.String},
				"style":         &graphql.Field{Type: graphql.String},
				"publishedDate": &graphql.Field{Type: dateTimeScalar},
				"updatedAt":     &graphql.Field{Type: dateTimeScalar},
				"isMember":      &graphql.Field{Type: graphql.Boolean},
				"isAdult":       &graphql.Field{Type: graphql.Boolean},
				"sections": &graphql.Field{
					Type: graphql.NewList(sectionType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeSectionWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterSections(current.Sections, where), nil
					},
				},
				"sectionsInInputOrder": &graphql.Field{
					Type: graphql.NewList(sectionType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeSectionWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterSections(current.SectionsInInputOrder, where), nil
					},
				},
				"categories": &graphql.Field{
					Type: graphql.NewList(categoryType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeCategoryWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterCategories(current.Categories, where), nil
					},
				},
				"categoriesInInputOrder": &graphql.Field{
					Type: graphql.NewList(categoryType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeCategoryWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterCategories(current.CategoriesInInputOrder, where), nil
					},
				},
				"writers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Writers, nil
					},
				},
				"writersInInputOrder": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).WritersInInputOrder, nil
					},
				},
				"photographers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Photographers, nil
					},
				},
				"camera_man": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).CameraMan, nil
					},
				},
				"designers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Designers, nil
					},
				},
				"engineers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Engineers, nil
					},
				},
				"vocals": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Vocals, nil
					},
				},
				"extend_byline": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).ExtendByline, nil
					},
				},
				"tags": &graphql.Field{
					Type: graphql.NewList(tagType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Tags, nil
					},
				},
				"tags_algo": &graphql.Field{
					Type: graphql.NewList(tagType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).TagsAlgo, nil
					},
				},
				"heroVideo": &graphql.Field{
					Type: videoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HeroVideo, nil
					},
				},
				"heroImage": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HeroImage, nil
					},
				},
				"heroCaption": &graphql.Field{Type: graphql.String},
				"brief":       &graphql.Field{Type: jsonScalar},
				"trimmedContent": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).TrimmedContent, nil
					},
				},
				"content": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Content, nil
					},
				},
				"relateds": &graphql.Field{
					Type: graphql.NewList(postType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Relateds, nil
					},
				},
				"relatedsInInputOrder": &graphql.Field{
					Type: graphql.NewList(postType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsInInputOrder, nil
					},
				},
				"relatedsOne": &graphql.Field{
					Type: postType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsOne, nil
					},
				},
				"relatedsTwo": &graphql.Field{
					Type: postType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsTwo, nil
					},
				},
				"redirect": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Redirect, nil
					},
				},
				"og_title": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgTitle, nil
					},
				},
				"og_image": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgImage, nil
					},
				},
				"og_description": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgDescription, nil
					},
				},
				"hiddenAdvertised": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HiddenAdvertised, nil
					},
				},
				"isAdvertised": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).IsAdvertised, nil
					},
				},
				"isFeatured": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).IsFeatured, nil
					},
				},
				"topics": &graphql.Field{
					Type: topicType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Topics, nil
					},
				},
			}
		}),
	})

	externalType := graphql.NewObject(graphql.ObjectConfig{
		Name: "External",
		Fields: graphql.Fields{
			"id":            &graphql.Field{Type: graphql.ID},
			"slug":          &graphql.Field{Type: graphql.String},
			"title":         &graphql.Field{Type: graphql.String},
			"thumb":         &graphql.Field{Type: graphql.String},
			"brief":         &graphql.Field{Type: graphql.String},
			"content":       &graphql.Field{Type: graphql.String},
			"publishedDate": &graphql.Field{Type: dateTimeScalar},
			"extend_byline": &graphql.Field{Type: graphql.String},
			"thumbCaption":  &graphql.Field{Type: graphql.String},
			"partner":       &graphql.Field{Type: partnerType},
			"updatedAt":     &graphql.Field{Type: dateTimeScalar},
		},
	})

	rootQuery := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"posts": &graphql.Field{
				Type: graphql.NewList(postType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(postOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ctx := p.Context
					where, err := decodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					result, err := queryPosts(ctx, where, orders, take, skip)
					if err != nil {
						return nil, err
					}
					return result, nil
				},
			},
			"postsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ctx := p.Context
					where, err := decodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					cnt, err := queryPostsCount(ctx, where)
					if err != nil {
						return nil, err
					}
					return cnt, nil
				},
			},
			"post": &graphql.Field{
				Type: postType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereUniqueInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ctx := p.Context
					where, err := decodePostWhereUnique(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return queryPostByUnique(ctx, where)
				},
			},
			"externals": &graphql.Field{
				Type: graphql.NewList(externalType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(externalOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: externalWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ctx := p.Context
					where, err := decodeExternalWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					result, err := queryExternals(ctx, where, orders, take, skip)
					if err != nil {
						return nil, err
					}
					return result, nil
				},
			},
			"externalsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: externalWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ctx := p.Context
					where, err := decodeExternalWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					cnt, err := queryExternalsCount(ctx, where)
					if err != nil {
						return nil, err
					}
					return cnt, nil
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{
		Query: rootQuery,
	})
}

func newJSONScalar() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "Arbitrary JSON value",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			return parseASTValue(valueAST)
		},
	})
}

func newDateTimeScalar() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name: "DateTime",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				return v.Value
			default:
				return nil
			}
		},
	})
}

func connectDB() (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL not set")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn := stdlib.OpenDB(*cfg)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return conn, nil
}

func newGraphQLHandler(schema graphql.Schema) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("only POST is supported at /query"))
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

func parseASTValue(value ast.Value) interface{} {
	switch v := value.(type) {
	case *ast.StringValue:
		return v.Value
	case *ast.IntValue:
		return v.Value
	case *ast.FloatValue:
		return v.Value
	case *ast.BooleanValue:
		return v.Value
	case *ast.ObjectValue:
		result := map[string]interface{}{}
		for _, field := range v.Fields {
			result[field.Name.Value] = parseASTValue(field.Value)
		}
		return result
	case *ast.ListValue:
		values := make([]interface{}, 0, len(v.Values))
		for _, item := range v.Values {
			values = append(values, parseASTValue(item))
		}
		return values
	default:
		return nil
	}
}

type imageMeta struct {
	id     int
	fileID string
	ext    string
	width  sql.NullInt64
	height sql.NullInt64
}

func queryPosts(ctx context.Context, where *postWhereInput, orders []orderRule, take, skip int) ([]post, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensurePostPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo" FROM "Post" p`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	if where != nil {
		if where.Slug != nil {
			if where.Slug.Equals != nil {
				conds = append(conds, fmt.Sprintf(`slug = $%d`, argIdx))
				args = append(args, *where.Slug.Equals)
				argIdx++
			}
			if len(where.Slug.In) > 0 {
				conds = append(conds, fmt.Sprintf(`slug = ANY($%d)`, argIdx))
				args = append(args, pqStringArray(where.Slug.In))
				argIdx++
			}
		}
		if where.State != nil && where.State.Equals != nil {
			conds = append(conds, fmt.Sprintf(`state = $%d`, argIdx))
			args = append(args, *where.State.Equals)
			argIdx++
		}
		if where.IsAdult != nil && where.IsAdult.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isAdult" = $%d`, argIdx))
			args = append(args, *where.IsAdult.Equals)
			argIdx++
		}
		if where.IsMember != nil && where.IsMember.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isMember" = $%d`, argIdx))
			args = append(args, *where.IsMember.Equals)
			argIdx++
		}
		if where.Sections != nil && where.Sections.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Post_sections\" ps JOIN \"Section\" s ON s.id = ps.\"B\" WHERE ps.\"A\" = p.id"
			if where.Sections.Some.Slug != nil && where.Sections.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND s.slug = $%d", argIdx)
				args = append(args, *where.Sections.Some.Slug.Equals)
				argIdx++
			}
			if where.Sections.Some.State != nil && where.Sections.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND s.state = $%d", argIdx)
				args = append(args, *where.Sections.Some.State.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
		if where.Categories != nil && where.Categories.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Category_posts\" cp JOIN \"Category\" c ON c.id = cp.\"A\" WHERE cp.\"B\" = p.id"
			if where.Categories.Some.Slug != nil && where.Categories.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND c.slug = $%d", argIdx)
				args = append(args, *where.Categories.Some.Slug.Equals)
				argIdx++
			}
			if where.Categories.Some.State != nil && where.Categories.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND c.state = $%d", argIdx)
				args = append(args, *where.Categories.Some.State.Equals)
				argIdx++
			}
			if where.Categories.Some.IsMemberOnly != nil && where.Categories.Some.IsMemberOnly.Equals != nil {
				sub += fmt.Sprintf(" AND c.\"isMemberOnly\" = $%d", argIdx)
				args = append(args, *where.Categories.Some.IsMemberOnly.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	// order
	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildOrderClause(orders[0]))
	} else {
		sb.WriteString(` ORDER BY "publishedDate" DESC`)
	}

	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	posts := []post{}
	for rows.Next() {
		var (
			p             post
			dbID          int
			heroImageID   sql.NullInt64
			heroVideoID   sql.NullInt64
			ogImageID     sql.NullInt64
			topicsID      sql.NullInt64
			relatedsOneID sql.NullInt64
			relatedsTwoID sql.NullInt64
			briefRaw      []byte
			contentRaw    []byte
		)
		if err := rows.Scan(
			&dbID,
			&p.Slug,
			&p.Title,
			&p.Subtitle,
			&p.State,
			&p.Style,
			&p.IsMember,
			&p.IsAdult,
			&p.PublishedDate,
			&p.UpdatedAt,
			&p.HeroCaption,
			&p.ExtendByline,
			&heroImageID,
			&heroVideoID,
			&briefRaw,
			&contentRaw,
			&p.Redirect,
			&p.OgTitle,
			&p.OgDescription,
			&p.HiddenAdvertised,
			&p.IsAdvertised,
			&p.IsFeatured,
			&topicsID,
			&ogImageID,
			&relatedsOneID,
			&relatedsTwoID,
		); err != nil {
			return nil, err
		}
		p.ID = strconv.Itoa(dbID)
		p.Brief = decodeJSONBytes(briefRaw)
		p.Content = decodeJSONBytes(contentRaw)
		p.TrimmedContent = p.Content
		p.Metadata = map[string]any{
			"heroImageID":   nullableInt(heroImageID),
			"ogImageID":     nullableInt(ogImageID),
			"heroVideoID":   nullableInt(heroVideoID),
			"topicsID":      nullableInt(topicsID),
			"relatedsOneID": nullableInt(relatedsOneID),
			"relatedsTwoID": nullableInt(relatedsTwoID),
		}
		posts = append(posts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(posts) == 0 {
		return posts, nil
	}
	if err := enrichPosts(ctx, posts); err != nil {
		return nil, err
	}
	return posts, nil
}

func queryPostsCount(ctx context.Context, where *postWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	where = ensurePostPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "Post" p`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	if where != nil {
		if where.Slug != nil && where.Slug.Equals != nil {
			conds = append(conds, fmt.Sprintf(`slug = $%d`, argIdx))
			args = append(args, *where.Slug.Equals)
			argIdx++
		}
		if where.State != nil && where.State.Equals != nil {
			conds = append(conds, fmt.Sprintf(`state = $%d`, argIdx))
			args = append(args, *where.State.Equals)
			argIdx++
		}
		if where.IsAdult != nil && where.IsAdult.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isAdult" = $%d`, argIdx))
			args = append(args, *where.IsAdult.Equals)
			argIdx++
		}
		if where.IsMember != nil && where.IsMember.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isMember" = $%d`, argIdx))
			args = append(args, *where.IsMember.Equals)
			argIdx++
		}
		if where.Sections != nil && where.Sections.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Post_sections\" ps JOIN \"Section\" s ON s.id = ps.\"B\" WHERE ps.\"A\" = p.id"
			if where.Sections.Some.Slug != nil && where.Sections.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND s.slug = $%d", argIdx)
				args = append(args, *where.Sections.Some.Slug.Equals)
				argIdx++
			}
			if where.Sections.Some.State != nil && where.Sections.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND s.state = $%d", argIdx)
				args = append(args, *where.Sections.Some.State.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
		if where.Categories != nil && where.Categories.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Category_posts\" cp JOIN \"Category\" c ON c.id = cp.\"A\" WHERE cp.\"B\" = p.id"
			if where.Categories.Some.Slug != nil && where.Categories.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND c.slug = $%d", argIdx)
				args = append(args, *where.Categories.Some.Slug.Equals)
				argIdx++
			}
			if where.Categories.Some.State != nil && where.Categories.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND c.state = $%d", argIdx)
				args = append(args, *where.Categories.Some.State.Equals)
				argIdx++
			}
			if where.Categories.Some.IsMemberOnly != nil && where.Categories.Some.IsMemberOnly.Equals != nil {
				sub += fmt.Sprintf(" AND c.\"isMemberOnly\" = $%d", argIdx)
				args = append(args, *where.Categories.Some.IsMemberOnly.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	var count int
	if err := db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func queryPostByUnique(ctx context.Context, where *postWhereUniqueInput) (*post, error) {
	if where == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo" FROM "Post" p WHERE `)
	args := []interface{}{}
	argIdx := 1
	if where.ID != nil {
		sb.WriteString(fmt.Sprintf("id = $%d", argIdx))
		args = append(args, *where.ID)
		argIdx++
	} else if where.Slug != nil {
		sb.WriteString(fmt.Sprintf("slug = $%d", argIdx))
		args = append(args, *where.Slug)
		argIdx++
	} else {
		return nil, nil
	}
	sb.WriteString(" LIMIT 1")

	var (
		p             post
		dbID          int
		heroImageID   sql.NullInt64
		heroVideoID   sql.NullInt64
		ogImageID     sql.NullInt64
		topicsID      sql.NullInt64
		relatedsOneID sql.NullInt64
		relatedsTwoID sql.NullInt64
		briefRaw      []byte
		contentRaw    []byte
	)

	err := db.QueryRowContext(ctx, sb.String(), args...).Scan(
		&dbID,
		&p.Slug,
		&p.Title,
		&p.Subtitle,
		&p.State,
		&p.Style,
		&p.IsMember,
		&p.IsAdult,
		&p.PublishedDate,
		&p.UpdatedAt,
		&p.HeroCaption,
		&p.ExtendByline,
		&heroImageID,
		&heroVideoID,
		&briefRaw,
		&contentRaw,
		&p.Redirect,
		&p.OgTitle,
		&p.OgDescription,
		&p.HiddenAdvertised,
		&p.IsAdvertised,
		&p.IsFeatured,
		&topicsID,
		&ogImageID,
		&relatedsOneID,
		&relatedsTwoID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.ID = strconv.Itoa(dbID)
	p.Brief = decodeJSONBytes(briefRaw)
	p.Content = decodeJSONBytes(contentRaw)
	p.TrimmedContent = p.Content
	p.Metadata = map[string]any{
		"heroImageID":   nullableInt(heroImageID),
		"ogImageID":     nullableInt(ogImageID),
		"heroVideoID":   nullableInt(heroVideoID),
		"topicsID":      nullableInt(topicsID),
		"relatedsOneID": nullableInt(relatedsOneID),
		"relatedsTwoID": nullableInt(relatedsTwoID),
	}
	posts := []post{p}
	if err := enrichPosts(ctx, posts); err != nil {
		return nil, err
	}
	p = posts[0]
	return &p, nil
}

func enrichPosts(ctx context.Context, posts []post) error {
	if len(posts) == 0 {
		return nil
	}
	postIDs := make([]int, 0, len(posts))
	for _, p := range posts {
		id, _ := strconvAtoiSafe(p.ID)
		if id == 0 {
			continue
		}
		postIDs = append(postIDs, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// maps
	postByID := map[int]*post{}
	for i := range posts {
		id, _ := strconvAtoiSafe(posts[i].ID)
		postByID[id] = &posts[i]
	}

	// gather image/video/topic/related ids
	imageIDs := []int{}
	videoIDs := []int{}
	topicIDs := []int{}
	relatedOneIDs := []int{}
	relatedTwoIDs := []int{}
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "heroImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
		if id := getMetaInt(p.Metadata, "ogImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
		if id := getMetaInt(p.Metadata, "heroVideoID"); id > 0 {
			videoIDs = append(videoIDs, id)
		}
		if id := getMetaInt(p.Metadata, "topicsID"); id > 0 {
			topicIDs = append(topicIDs, id)
		}
		if id := getMetaInt(p.Metadata, "relatedsOneID"); id > 0 {
			relatedOneIDs = append(relatedOneIDs, id)
		}
		if id := getMetaInt(p.Metadata, "relatedsTwoID"); id > 0 {
			relatedTwoIDs = append(relatedTwoIDs, id)
		}
	}

	sectionsMap, err := fetchSections(ctx, postIDs)
	if err != nil {
		return err
	}
	categoriesMap, err := fetchCategories(ctx, postIDs)
	if err != nil {
		return err
	}
	roleMapWriters, _ := fetchContacts(ctx, "_Post_writers", postIDs)
	roleMapPhotographers, _ := fetchContacts(ctx, "_Post_photographers", postIDs)
	roleMapCamera, _ := fetchContacts(ctx, "_Post_camera_man", postIDs)
	roleMapDesigners, _ := fetchContacts(ctx, "_Post_designers", postIDs)
	roleMapEngineers, _ := fetchContacts(ctx, "_Post_engineers", postIDs)
	roleMapVocals, _ := fetchContacts(ctx, "_Post_vocals", postIDs)

	tagsMap, _ := fetchTags(ctx, "_Post_tags", postIDs)
	tagsAlgoMap, _ := fetchTags(ctx, "_Post_tags_algo", postIDs)

	relatedsMap, relatedImageIDs, err := fetchRelatedPosts(ctx, postIDs)
	if err != nil {
		return err
	}
	imageIDs = append(imageIDs, relatedImageIDs...)

	// fetch relatedOne/Two posts if needed
	relatedSinglesIDs := append(relatedOneIDs, relatedTwoIDs...)
	relatedSinglePosts := map[int]post{}
	if len(relatedSinglesIDs) > 0 {
		sps, imgIDs, err := fetchPostsByIDs(ctx, relatedSinglesIDs)
		if err != nil {
			return err
		}
		for _, sp := range sps {
			id, _ := strconvAtoiSafe(sp.ID)
			relatedSinglePosts[id] = sp
		}
		imageIDs = append(imageIDs, imgIDs...)
	}

	// videos
	videoMap, videoImageIDs, err := fetchVideos(ctx, videoIDs)
	if err == nil {
		imageIDs = append(imageIDs, videoImageIDs...)
	}

	// topics
	topicMap, _ := fetchTopics(ctx, topicIDs)

	// images
	imageMap, err := fetchImages(ctx, imageIDs)
	if err != nil {
		return err
	}

	for i := range posts {
		p := &posts[i]
		id, _ := strconvAtoiSafe(p.ID)
		p.Sections = sectionsMap[id]
		p.SectionsInInputOrder = sectionsMap[id]
		p.Categories = categoriesMap[id]
		p.CategoriesInInputOrder = categoriesMap[id]
		p.Writers = roleMapWriters[id]
		p.WritersInInputOrder = roleMapWriters[id]
		p.Photographers = roleMapPhotographers[id]
		p.CameraMan = roleMapCamera[id]
		p.Designers = roleMapDesigners[id]
		p.Engineers = roleMapEngineers[id]
		p.Vocals = roleMapVocals[id]
		p.Tags = tagsMap[id]
		p.TagsAlgo = tagsAlgoMap[id]
		p.Relateds = relatedsMap[id]
		p.RelatedsInInputOrder = relatedsMap[id]
		if idImg := getMetaInt(p.Metadata, "heroImageID"); idImg > 0 {
			p.HeroImage = imageMap[idImg]
		}
		if idImg := getMetaInt(p.Metadata, "ogImageID"); idImg > 0 {
			p.OgImage = imageMap[idImg]
		}
		if vid := getMetaInt(p.Metadata, "heroVideoID"); vid > 0 {
			p.HeroVideo = videoMap[vid]
		}
		if tid := getMetaInt(p.Metadata, "topicsID"); tid > 0 {
			if t, ok := topicMap[tid]; ok {
				p.Topics = &t
			}
		}
		if r1 := getMetaInt(p.Metadata, "relatedsOneID"); r1 > 0 {
			if rp, ok := relatedSinglePosts[r1]; ok {
				p.RelatedsOne = &rp
			}
		}
		if r2 := getMetaInt(p.Metadata, "relatedsTwoID"); r2 > 0 {
			if rp, ok := relatedSinglePosts[r2]; ok {
				p.RelatedsTwo = &rp
			}
		}
	}
	return nil
}

func fetchSections(ctx context.Context, postIDs []int) (map[int][]section, error) {
	result := map[int][]section{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `SELECT ps."A" as post_id, s.id, s.name, s.slug, s.state FROM "_Post_sections" ps JOIN "Section" s ON s.id = ps."B" WHERE ps."A" = ANY($1)`
	rows, err := db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var s section
		if err := rows.Scan(&pid, &s.ID, &s.Name, &s.Slug, &s.State); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], s)
	}
	return result, rows.Err()
}

func fetchCategories(ctx context.Context, postIDs []int) (map[int][]category, error) {
	result := map[int][]category{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `SELECT cp."B" as post_id, c.id, c.name, c.slug, c.state, c."isMemberOnly" FROM "_Category_posts" cp JOIN "Category" c ON c.id = cp."A" WHERE cp."B" = ANY($1)`
	rows, err := db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c category
		if err := rows.Scan(&pid, &c.ID, &c.Name, &c.Slug, &c.State, &c.IsMemberOnly); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], c)
	}
	return result, rows.Err()
}

func fetchContacts(ctx context.Context, table string, postIDs []int) (map[int][]contact, error) {
	result := map[int][]contact{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := fmt.Sprintf(`SELECT t."B" as post_id, c.id, c.name FROM "%s" t JOIN "Contact" c ON c.id = t."A" WHERE t."B" = ANY($1)`, table)
	rows, err := db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c contact
		if err := rows.Scan(&pid, &c.ID, &c.Name); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], c)
	}
	return result, rows.Err()
}

func fetchTags(ctx context.Context, table string, postIDs []int) (map[int][]tag, error) {
	result := map[int][]tag{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := fmt.Sprintf(`SELECT t."A" as post_id, tg.id, tg.name, tg.slug FROM "%s" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`, table)
	rows, err := db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var t tag
		if err := rows.Scan(&pid, &t.ID, &t.Name, &t.Slug); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], t)
	}
	return result, rows.Err()
}

func fetchRelatedPosts(ctx context.Context, postIDs []int) (map[int][]post, []int, error) {
	result := map[int][]post{}
	imageIDs := []int{}
	if len(postIDs) == 0 {
		return result, imageIDs, nil
	}
	query := `
		SELECT r."A" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."B"
		WHERE r."A" = ANY($1)
		UNION
		SELECT r."B" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."A"
		WHERE r."B" = ANY($1)
	`
	rows, err := db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var rp post
		var dbID int
		var heroID sql.NullInt64
		if err := rows.Scan(&pid, &dbID, &rp.Slug, &rp.Title, &heroID); err != nil {
			return result, imageIDs, err
		}
		rp.ID = strconv.Itoa(dbID)
		if heroID.Valid {
			imageIDs = append(imageIDs, int(heroID.Int64))
			rp.Metadata = map[string]any{"heroImageID": int(heroID.Int64)}
		}
		result[pid] = append(result[pid], rp)
	}
	return result, imageIDs, rows.Err()
}

func fetchPostsByIDs(ctx context.Context, ids []int) ([]post, []int, error) {
	result := []post{}
	imageIDs := []int{}
	if len(ids) == 0 {
		return result, imageIDs, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, slug, title, "heroImage" FROM "Post" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var p post
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&dbID, &p.Slug, &p.Title, &hero); err != nil {
			return result, imageIDs, err
		}
		p.ID = strconv.Itoa(dbID)
		if hero.Valid {
			imageIDs = append(imageIDs, int(hero.Int64))
			p.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result = append(result, p)
	}
	return result, imageIDs, rows.Err()
}

func fetchVideos(ctx context.Context, videoIDs []int) (map[int]*video, []int, error) {
	result := map[int]*video{}
	imageIDs := []int{}
	if len(videoIDs) == 0 {
		return result, imageIDs, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, "urlOriginal", "heroImage" FROM "Video" WHERE id = ANY($1)`, pqIntArray(videoIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var v video
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&dbID, &v.VideoSrc, &hero); err != nil {
			return result, imageIDs, err
		}
		v.ID = strconv.Itoa(dbID)
		if hero.Valid {
			imageIDs = append(imageIDs, int(hero.Int64))
			v.HeroImage = &photo{}
			v.HeroImage.ImageFile = imageFile{}
			v.HeroImage.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result[dbID] = &v
	}
	return result, imageIDs, rows.Err()
}

func fetchTopics(ctx context.Context, ids []int) (map[int]topic, error) {
	result := map[int]topic{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, slug FROM "Topic" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var t topic
		if err := rows.Scan(&id, &t.Slug); err != nil {
			return result, err
		}
		result[id] = t
	}
	return result, rows.Err()
}

func fetchImages(ctx context.Context, ids []int) (map[int]*photo, error) {
	result := map[int]*photo{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, COALESCE("imageFile_id", ''), COALESCE("imageFile_extension", ''), "imageFile_width", "imageFile_height" FROM "Image" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var im imageMeta
		if err := rows.Scan(&im.id, &im.fileID, &im.ext, &im.width, &im.height); err != nil {
			return result, err
		}
		photo := photo{
			ID: strconv.Itoa(im.id),
			ImageFile: imageFile{
				Width:  int(im.width.Int64),
				Height: int(im.height.Int64),
			},
		}
		photo.Resized = buildResizedURLs(im.fileID, im.ext)
		photo.ResizedWebp = buildResizedURLs(im.fileID, "webp")
		result[im.id] = &photo
	}
	return result, rows.Err()
}

func buildResizedURLs(fileID, ext string) resized {
	if fileID == "" {
		return resized{}
	}
	if ext == "" {
		ext = "jpg"
	}
	host := staticsHost
	makeURL := func(size string, extension string) string {
		if size == "" {
			return fmt.Sprintf("%s/%s.%s", host, fileID, extension)
		}
		return fmt.Sprintf("%s/%s-%s.%s", host, fileID, size, extension)
	}
	return resized{
		Original: makeURL("", ext),
		W480:     makeURL("w480", ext),
		W800:     makeURL("w800", ext),
		W1200:    makeURL("w1200", ext),
		W1600:    makeURL("w1600", ext),
		W2400:    makeURL("w2400", ext),
	}
}

func queryExternals(ctx context.Context, where *externalWhereInput, orders []orderRule, take, skip int) ([]external, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureExternalPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT e.id, e.slug, e.title, e.state, e."publishedDate", e."extend_byline", e.thumb, e."thumbCaption", e.brief, e.content, e.partner, e."updatedAt" FROM "External" e`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	if where != nil {
		if where.Slug != nil && where.Slug.Equals != nil {
			conds = append(conds, fmt.Sprintf(`e.slug = $%d`, argIdx))
			args = append(args, *where.Slug.Equals)
			argIdx++
		}
		if where.State != nil && where.State.Equals != nil {
			conds = append(conds, fmt.Sprintf(`e.state = $%d`, argIdx))
			args = append(args, *where.State.Equals)
			argIdx++
		}
		if where.Partner != nil && where.Partner.Slug != nil && where.Partner.Slug.Equals != nil {
			sb.WriteString(` JOIN "Partner" p ON p.id = e.partner`)
			conds = append(conds, fmt.Sprintf(`p.slug = $%d`, argIdx))
			args = append(args, *where.Partner.Slug.Equals)
			argIdx++
		}
	}
	// put non-null publishedDate first when ordering by publishedDate (default case)
	orderUsesPublished := len(orders) == 0 || (len(orders) > 0 && orders[0].Field == "publishedDate")
	if orderUsesPublished {
		conds = append(conds, `e."publishedDate" IS NOT NULL`)
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildExternalOrder(orders[0]))
	} else {
		sb.WriteString(` ORDER BY e."publishedDate" DESC`)
	}
	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []external{}
	partnerIDs := []int{}
	externalIDs := []int{}
	for rows.Next() {
		var ext external
		var partnerID sql.NullInt64
		var dbID int
		var pubAt, updAt sql.NullTime
		if err := rows.Scan(&dbID, &ext.Slug, &ext.Title, &ext.State, &pubAt, &ext.ExtendByline, &ext.Thumb, &ext.ThumbCaption, &ext.Brief, &ext.Content, &partnerID, &updAt); err != nil {
			return nil, err
		}
		ext.ID = strconv.Itoa(dbID)
		if pubAt.Valid {
			ext.PublishedDate = pubAt.Time.UTC().Format(time.RFC3339)
		}
		if updAt.Valid {
			ext.UpdatedAt = updAt.Time.UTC().Format(time.RFC3339)
		}
		externalIDs = append(externalIDs, dbID)
		if partnerID.Valid {
			ext.Metadata = map[string]any{"partnerID": int(partnerID.Int64)}
			partnerIDs = append(partnerIDs, int(partnerID.Int64))
		}
		result = append(result, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	partners, _ := fetchPartners(ctx, partnerIDs)
	tagsMap, _ := fetchExternalTags(ctx, "_External_tags", externalIDs)
	for i := range result {
		if pid := getMetaInt(result[i].Metadata, "partnerID"); pid > 0 {
			result[i].Partner = partners[pid]
		}
		idInt, _ := strconvAtoiSafe(result[i].ID)
		result[i].Tags = tagsMap[idInt]
	}
	return result, nil
}

func queryExternalsCount(ctx context.Context, where *externalWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	where = ensureExternalPublished(where)
	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "External" e`)
	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	if where != nil {
		if where.Slug != nil && where.Slug.Equals != nil {
			conds = append(conds, fmt.Sprintf(`e.slug = $%d`, argIdx))
			args = append(args, *where.Slug.Equals)
			argIdx++
		}
		if where.State != nil && where.State.Equals != nil {
			conds = append(conds, fmt.Sprintf(`e.state = $%d`, argIdx))
			args = append(args, *where.State.Equals)
			argIdx++
		}
		if where.Partner != nil && where.Partner.Slug != nil && where.Partner.Slug.Equals != nil {
			sb.WriteString(` JOIN "Partner" p ON p.id = e.partner`)
			conds = append(conds, fmt.Sprintf(`p.slug = $%d`, argIdx))
			args = append(args, *where.Partner.Slug.Equals)
			argIdx++
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	var count int
	if err := db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func fetchPartners(ctx context.Context, ids []int) (map[int]*partner, error) {
	result := map[int]*partner{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, slug, name, "showOnIndex", COALESCE("showThumb", true), COALESCE("showBrief", false) FROM "Partner" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var p partner
		var dbID int
		if err := rows.Scan(&dbID, &p.Slug, &p.Name, &p.ShowOnIndex, &p.ShowThumb, &p.ShowBrief); err != nil {
			return result, err
		}
		p.ID = strconv.Itoa(dbID)
		result[dbID] = &p
	}
	return result, rows.Err()
}

func fetchExternalTags(ctx context.Context, table string, externalIDs []int) (map[int][]tag, error) {
	result := map[int][]tag{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT t."A" as external_id, tg.id, tg.name, tg.slug FROM "%s" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`, table), pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var tg tag
		if err := rows.Scan(&eid, &tg.ID, &tg.Name, &tg.Slug); err != nil {
			return result, err
		}
		result[eid] = append(result[eid], tg)
	}
	return result, rows.Err()
}

func decodeJSONBytes(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func pqIntArray(ids []int) interface{} {
	arr := make([]int64, len(ids))
	for i, id := range ids {
		arr[i] = int64(id)
	}
	return arr
}

func pqStringArray(arr []string) interface{} {
	return arr
}

func buildOrderClause(rule orderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}
	switch rule.Field {
	case "publishedDate":
		return fmt.Sprintf(`"publishedDate" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`"updatedAt" %s`, dir)
	case "title":
		return fmt.Sprintf(`"title" %s`, dir)
	default:
		return `"publishedDate" DESC`
	}
}

func buildExternalOrder(rule orderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}
	switch rule.Field {
	case "publishedDate":
		return fmt.Sprintf(`e."publishedDate" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`e."updatedAt" %s`, dir)
	default:
		return `e."publishedDate" DESC`
	}
}

func getMetaInt(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		}
	}
	return 0
}

func nullableInt(v sql.NullInt64) int {
	if v.Valid {
		return int(v.Int64)
	}
	return 0
}

func strconvAtoiSafe(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

func mapKeys(m map[int]struct{}) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
func buildSampleData() dataStore {
	sections := []section{
		{ID: "sec-politics", Name: "Politics", Slug: "politics", State: "active"},
		{ID: "sec-society", Name: "Society", Slug: "society", State: "active"},
		{ID: "sec-culture", Name: "Culture", Slug: "culture", State: "inactive"},
	}

	categories := []category{
		{ID: "cat-1", Name: "Focus", Slug: "focus", State: "active", IsMemberOnly: false, Sections: sections[:2]},
		{ID: "cat-2", Name: "Feature", Slug: "feature", State: "active", IsMemberOnly: true, Sections: sections[1:]},
	}

	contacts := []contact{
		{ID: "writer-1", Name: "Reporter A"},
		{ID: "writer-2", Name: "Reporter B"},
	}

	photoA := &photo{
		ImageFile: imageFile{Width: 1200, Height: 800},
		Resized: resized{
			Original: "https://images.mirror-media.xyz/originalA.jpg",
			W480:     "https://images.mirror-media.xyz/480A.jpg",
			W800:     "https://images.mirror-media.xyz/800A.jpg",
			W1200:    "https://images.mirror-media.xyz/1200A.jpg",
			W1600:    "https://images.mirror-media.xyz/1600A.jpg",
			W2400:    "https://images.mirror-media.xyz/2400A.jpg",
		},
		ResizedWebp: resized{
			Original: "https://images.mirror-media.xyz/originalA.webp",
			W480:     "https://images.mirror-media.xyz/480A.webp",
			W800:     "https://images.mirror-media.xyz/800A.webp",
			W1200:    "https://images.mirror-media.xyz/1200A.webp",
			W1600:    "https://images.mirror-media.xyz/1600A.webp",
			W2400:    "https://images.mirror-media.xyz/2400A.webp",
		},
	}

	videoA := &video{
		ID:       "video-1",
		VideoSrc: "https://videos.mirror-media.xyz/video1.mp4",
		HeroImage: &photo{
			Resized: resized{Original: "https://images.mirror-media.xyz/video-cover.jpg"},
		},
	}

	tagNews := tag{ID: "tag-news", Name: "新聞", Slug: "news"}
	tagAI := tag{ID: "tag-ai", Name: "AI", Slug: "ai"}

	postA := post{
		ID:                     "post-1",
		Slug:                   "story-a",
		Title:                  "第一篇文章",
		Subtitle:               "副標題 A",
		State:                  "published",
		Style:                  "article",
		PublishedDate:          "2024-12-01T10:00:00Z",
		UpdatedAt:              "2024-12-02T08:00:00Z",
		IsMember:               false,
		IsAdult:                false,
		Sections:               []section{sections[0], sections[1]},
		SectionsInInputOrder:   []section{sections[1], sections[0]},
		Categories:             []category{categories[0]},
		CategoriesInInputOrder: []category{categories[0]},
		Writers:                contacts[:1],
		WritersInInputOrder:    contacts[:1],
		Photographers:          []contact{contacts[1]},
		CameraMan:              []contact{},
		Designers:              []contact{},
		Engineers:              []contact{},
		Vocals:                 []contact{},
		ExtendByline:           "延伸署名 A",
		Tags:                   []tag{tagNews},
		TagsAlgo:               []tag{tagAI},
		HeroVideo:              videoA,
		HeroImage:              photoA,
		HeroCaption:            "圖說 A",
		Brief:                  map[string]any{"text": "這是簡介"},
		TrimmedContent:         map[string]any{"text": "節錄內容"},
		Content:                map[string]any{"text": "完整內容"},
		Redirect:               "",
		OgTitle:                "OG Title A",
		OgImage:                photoA,
		OgDescription:          "OG Description A",
		HiddenAdvertised:       false,
		IsAdvertised:           false,
		IsFeatured:             true,
		Topics:                 &topic{Slug: "topic-a"},
	}

	postB := post{
		ID:                     "post-2",
		Slug:                   "story-b",
		Title:                  "第二篇文章",
		Subtitle:               "副標題 B",
		State:                  "published",
		Style:                  "article",
		PublishedDate:          "2024-11-15T10:00:00Z",
		UpdatedAt:              "2024-11-16T08:00:00Z",
		IsMember:               true,
		IsAdult:                false,
		Sections:               []section{sections[1]},
		SectionsInInputOrder:   []section{sections[1]},
		Categories:             []category{categories[1]},
		CategoriesInInputOrder: []category{categories[1]},
		Writers:                contacts,
		WritersInInputOrder:    contacts,
		Photographers:          []contact{},
		CameraMan:              []contact{},
		Designers:              []contact{},
		Engineers:              []contact{},
		Vocals:                 []contact{},
		ExtendByline:           "延伸署名 B",
		Tags:                   []tag{tagAI},
		TagsAlgo:               []tag{tagNews},
		HeroVideo:              nil,
		HeroImage:              photoA,
		HeroCaption:            "圖說 B",
		Brief:                  map[string]any{"text": "這是第二篇的簡介"},
		TrimmedContent:         map[string]any{"text": "第二篇節錄"},
		Content:                map[string]any{"text": "第二篇完整內容"},
		Redirect:               "",
		OgTitle:                "OG Title B",
		OgImage:                photoA,
		OgDescription:          "OG Description B",
		HiddenAdvertised:       false,
		IsAdvertised:           true,
		IsFeatured:             false,
		Topics:                 &topic{Slug: "topic-b"},
	}

	// build related references
	postA.Relateds = []post{postB}
	postA.RelatedsInInputOrder = []post{postB}
	postA.RelatedsOne = &postB
	postA.RelatedsTwo = nil

	postB.Relateds = []post{postA}
	postB.RelatedsInInputOrder = []post{postA}
	postB.RelatedsOne = &postA
	postB.RelatedsTwo = nil

	partnerA := &partner{
		ID:          "partner-1",
		Slug:        "mirror-media",
		Name:        "鏡傳媒",
		ShowOnIndex: true,
		ShowThumb:   true,
		ShowBrief:   true,
	}

	externals := []external{
		{
			ID:            "external-1",
			Slug:          "external-story",
			Partner:       partnerA,
			Title:         "外部文章一",
			State:         "published",
			PublishedDate: "2024-12-10T09:00:00Z",
			ExtendByline:  "外部作者",
			Thumb:         "https://images.mirror-media.xyz/external-thumb.jpg",
			ThumbCaption:  "外部圖片說明",
			Brief:         "外部文章摘要",
			Content:       "外部文章完整內容",
			UpdatedAt:     "2024-12-11T09:00:00Z",
			Tags:          []tag{tagNews},
			Relateds:      []post{postA},
			Metadata:      map[string]any{},
		},
		{
			ID:            "external-2",
			Slug:          "external-ai",
			Partner:       partnerA,
			Title:         "AI 外部文章",
			State:         "draft",
			PublishedDate: "2024-12-08T09:00:00Z",
			ExtendByline:  "作者 B",
			Thumb:         "https://images.mirror-media.xyz/external-thumb2.jpg",
			ThumbCaption:  "外部圖片說明 2",
			Brief:         "AI 相關摘要",
			Content:       "AI 相關內容",
			UpdatedAt:     "2024-12-09T09:00:00Z",
			Tags:          []tag{tagAI},
			Relateds:      []post{postB},
			Metadata:      map[string]any{},
		},
	}

	return dataStore{
		Posts:     []post{postA, postB},
		Externals: externals,
	}
}

func decodePostWhere(input interface{}) (*postWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where postWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("post where: %w", err)
	}
	return &where, nil
}

func decodePostWhereUnique(input interface{}) (*postWhereUniqueInput, error) {
	if input == nil {
		return nil, nil
	}
	var where postWhereUniqueInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("post unique where: %w", err)
	}
	return &where, nil
}

func decodeExternalWhere(input interface{}) (*externalWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where externalWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("external where: %w", err)
	}
	return &where, nil
}

func ensurePostPublished(where *postWhereInput) *postWhereInput {
	if where == nil {
		where = &postWhereInput{}
	}
	if where.State == nil {
		where.State = &stringFilter{Equals: ptrString("published")}
	}
	return where
}

func ensureExternalPublished(where *externalWhereInput) *externalWhereInput {
	if where == nil {
		where = &externalWhereInput{}
	}
	if where.State == nil {
		where.State = &stringFilter{Equals: ptrString("published")}
	}
	return where
}

func ptrString(s string) *string {
	return &s
}

func decodeSectionWhere(input interface{}) (*sectionWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where sectionWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, err
	}
	return &where, nil
}

func decodeCategoryWhere(input interface{}) (*categoryWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where categoryWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, err
	}
	return &where, nil
}

func decodeInto(input interface{}, target interface{}) error {
	cfg := &mapstructure.DecoderConfig{
		TagName: "mapstructure",
		Result:  target,
	}
	decoder, err := mapstructure.NewDecoder(cfg)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}

func filterPosts(items []post, where *postWhereInput) []post {
	result := make([]post, 0, len(items))
	for _, p := range items {
		if matchesPost(&p, where) {
			result = append(result, p)
		}
	}
	return result
}

func filterExternals(items []external, where *externalWhereInput) []external {
	result := make([]external, 0, len(items))
	for _, e := range items {
		if matchesExternal(&e, where) {
			result = append(result, e)
		}
	}
	return result
}

func matchesPost(p *post, where *postWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(p.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(p.State, where.State) {
		return false
	}
	if !matchesBooleanFilter(p.IsAdult, where.IsAdult) {
		return false
	}
	if !matchesBooleanFilter(p.IsMember, where.IsMember) {
		return false
	}
	if !matchesSectionManyFilter(p.Sections, where.Sections) {
		return false
	}
	if !matchesCategoryManyFilter(p.Categories, where.Categories) {
		return false
	}
	return true
}

func matchesExternal(e *external, where *externalWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(e.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(e.State, where.State) {
		return false
	}
	if where.Partner != nil {
		slug := ""
		if e.Partner != nil {
			slug = e.Partner.Slug
		}
		if !matchesStringFilter(slug, where.Partner.Slug) {
			return false
		}
	}
	return true
}

func matchesSectionManyFilter(items []section, filter *sectionManyRelationFilter) bool {
	if filter == nil || filter.Some == nil {
		return true
	}
	for _, s := range items {
		if matchesSectionWhere(&s, filter.Some) {
			return true
		}
	}
	return false
}

func matchesCategoryManyFilter(items []category, filter *categoryManyRelationFilter) bool {
	if filter == nil || filter.Some == nil {
		return true
	}
	for _, c := range items {
		if matchesCategoryWhere(&c, filter.Some) {
			return true
		}
	}
	return false
}

func matchesSectionWhere(s *section, where *sectionWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(s.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(s.State, where.State) {
		return false
	}
	return true
}

func matchesCategoryWhere(c *category, where *categoryWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(c.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(c.State, where.State) {
		return false
	}
	if !matchesBooleanFilter(c.IsMemberOnly, where.IsMemberOnly) {
		return false
	}
	return true
}

func matchesStringFilter(value string, filter *stringFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Equals != nil && value != *filter.Equals {
		return false
	}
	if len(filter.In) > 0 {
		found := false
		for _, item := range filter.In {
			if value == item {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if filter.Not != nil && matchesStringFilter(value, filter.Not) {
		return false
	}
	return true
}

func matchesBooleanFilter(value bool, filter *booleanFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Equals != nil && value != *filter.Equals {
		return false
	}
	return true
}

func filterSections(items []section, where *sectionWhereInput) []section {
	if where == nil {
		return items
	}
	result := make([]section, 0, len(items))
	for _, s := range items {
		if matchesSectionWhere(&s, where) {
			result = append(result, s)
		}
	}
	return result
}

func filterCategories(items []category, where *categoryWhereInput) []category {
	if where == nil {
		return items
	}
	result := make([]category, 0, len(items))
	for _, c := range items {
		if matchesCategoryWhere(&c, where) {
			result = append(result, c)
		}
	}
	return result
}

func parseOrderRules(input interface{}) []orderRule {
	rules := []orderRule{}
	list, ok := input.([]interface{})
	if !ok {
		return rules
	}
	for _, item := range list {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for field, dir := range entry {
			rules = append(rules, orderRule{
				Field:     field,
				Direction: fmt.Sprintf("%v", dir),
			})
		}
	}
	return rules
}

func sortPosts(items []post, orders []orderRule) {
	if len(orders) == 0 {
		return
	}
	rule := orders[0]
	dir := strings.ToLower(rule.Direction)
	switch rule.Field {
	case "publishedDate":
		sort.Slice(items, func(i, j int) bool {
			if dir == "desc" {
				return items[i].PublishedDate > items[j].PublishedDate
			}
			return items[i].PublishedDate < items[j].PublishedDate
		})
	case "updatedAt":
		sort.Slice(items, func(i, j int) bool {
			if dir == "desc" {
				return items[i].UpdatedAt > items[j].UpdatedAt
			}
			return items[i].UpdatedAt < items[j].UpdatedAt
		})
	case "title":
		sort.Slice(items, func(i, j int) bool {
			if dir == "desc" {
				return items[i].Title > items[j].Title
			}
			return items[i].Title < items[j].Title
		})
	}
}

func sortExternals(items []external, orders []orderRule) {
	if len(orders) == 0 {
		return
	}
	rule := orders[0]
	dir := strings.ToLower(rule.Direction)
	switch rule.Field {
	case "publishedDate":
		sort.Slice(items, func(i, j int) bool {
			if dir == "desc" {
				return items[i].PublishedDate > items[j].PublishedDate
			}
			return items[i].PublishedDate < items[j].PublishedDate
		})
	case "updatedAt":
		sort.Slice(items, func(i, j int) bool {
			if dir == "desc" {
				return items[i].UpdatedAt > items[j].UpdatedAt
			}
			return items[i].UpdatedAt < items[j].UpdatedAt
		})
	}
}

func parsePagination(args map[string]interface{}) (take int, skip int) {
	if raw, ok := args["take"]; ok {
		take = asInt(raw)
	}
	if raw, ok := args["skip"]; ok {
		skip = asInt(raw)
	}
	if skip < 0 {
		skip = 0
	}
	return
}

func applyPagination[T any](items []T, take int, skip int) []T {
	if skip >= len(items) {
		return []T{}
	}
	end := len(items)
	if take > 0 && skip+take < end {
		end = skip + take
	}
	return items[skip:end]
}

func asInt(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func normalizePost(src interface{}) post {
	switch v := src.(type) {
	case post:
		return v
	case *post:
		if v == nil {
			return post{}
		}
		return *v
	default:
		return post{}
	}
}

func findPost(where *postWhereUniqueInput) *post {
	if where == nil {
		return nil
	}
	for idx := range store.Posts {
		p := &store.Posts[idx]
		if where.ID != nil && p.ID == *where.ID {
			return p
		}
		if where.Slug != nil && p.Slug == *where.Slug {
			return p
		}
	}
	return nil
}
