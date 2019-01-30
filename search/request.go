package search

import (
  "context"
  "fmt"
  "net/http"
  "strconv"
  "strings"

  "github.com/Liquid-Labs/go-rest/rest"
)

type SearchFunc func(*rest.SearchParams, context.Context, []*JoinData) (interface{}, int64, rest.RestError)

func ExtractSearchParamsFromUrl(w http.ResponseWriter, r *http.Request) (*rest.SearchParams, rest.RestError) {
  var sp rest.SearchParams

  scopes := r.FormValue("scopes")
  if scopes != "" {
    sp.Scopes = strings.Split(scopes, ",")
  } else {
    sp.Scopes = make([]string, 0)
  }
  terms := r.FormValue("terms")
  if terms != "" {
    sp.Terms = strings.Split(terms, ",")
  } else {
    sp.Terms = make([]string, 0)
  }
  sp.Sort = r.FormValue("sort")

  pageIndexStr := r.FormValue("pageIndex")
  var pageIndex int
  var err error
  if pageIndexStr != "" {
    if pageIndex, err = strconv.Atoi(pageIndexStr); err != nil {
      return nil, rest.HandleError(w, rest.BadRequestError(fmt.Sprintf("Could not parse pageIndex: %s", pageIndexStr), err))
    }
  } else {
    pageIndex = 1
  }

  itemsPerPageStr := r.FormValue("itemsPerPage")
  var itemsPerPage int
  if itemsPerPageStr != "" {
    if itemsPerPage, err = strconv.Atoi(itemsPerPageStr); err != nil {
      return nil, rest.HandleError(w, rest.BadRequestError(fmt.Sprintf("Could not parse itemsPerPage: %s", itemsPerPageStr), err))
    }
    if itemsPerPage < 20 {
      itemsPerPage = 20
    } else if itemsPerPage > 250 {
      itemsPerPage = 250
    }
  } else {
    itemsPerPage = 100
  }
  sp.PageInfo = &rest.PageInfo{PageIndex: pageIndex, ItemsPerPage: itemsPerPage}

  return &sp, nil
}

func DoList(w http.ResponseWriter, r *http.Request, contextJoins []*JoinData, searchFunc SearchFunc, resourceName string) {
  // note that the auth is handled by the caller because they caller may want to use the token in setting the contextJoins
  var sp *rest.SearchParams
  var restErr rest.RestError
  if sp, restErr = rest.ExtractSearchParamsFromUrl(w, r); restErr != nil {
    // HTTP response is already set
    return
  }

  items, count, err := searchFunc(sp, r.Context(), contextJoins)
  if err != nil {
    rest.HandleError(w, err)
    return
  }

  sp.SetTotalPages(count)

  rest.StandardResponse(w, items, resourceName + ` retrieved.`, sp)
}
