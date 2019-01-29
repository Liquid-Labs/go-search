package search

import (
  "context"
  "fmt"
  "net/http"
  "strconv"
  "strings"

  "github.com/Liquid-Labs/catalyst-firewrap/go/fireauth"
)

type PageInfo struct {
  // 1-based index
  PageIndex      int   `json:"pageIndex"`
  ItemsPerPage   int   `json:"itemsPerPage"`
  TotalItemCount int64 `json:"totalItemCount"`
  TotalPageCount int64 `json:"totalPageCount"`
}

type SearchParams struct {
  Scopes   []string  `json:"scopes"`
  Terms    []string  `json:"terms"`
  Sort     string    `json:"sort"`
  PageInfo *PageInfo `json:"pageInfo"`
}

func (sp *SearchParams) EnsureSingleScope() (RestError) {
  if len(sp.Scopes) > 1 {
    return BadRequestError("We currently only support a single scope.", nil)
  } else if (len(sp.Scopes) == 0) {
    return BadRequestError("No scope specified.", nil)
  } else {
    return nil
  }
}

type SearchFunc func(*SearchParams, context.Context, []*JoinData) (interface{}, int64, RestError)

func ExtractSearchParamsFromUrl(w http.ResponseWriter, r *http.Request) (*SearchParams, RestError) {
  var sp SearchParams

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
      return nil, HandleError(w, BadRequestError(fmt.Sprintf("Could not parse pageIndex: %s", pageIndexStr), err))
    }
  } else {
    pageIndex = 1
  }

  itemsPerPageStr := r.FormValue("itemsPerPage")
  var itemsPerPage int
  if itemsPerPageStr != "" {
    if itemsPerPage, err = strconv.Atoi(itemsPerPageStr); err != nil {
      return nil, HandleError(w, BadRequestError(fmt.Sprintf("Could not parse itemsPerPage: %s", itemsPerPageStr), err))
    }
    if itemsPerPage < 20 {
      itemsPerPage = 20
    } else if itemsPerPage > 250 {
      itemsPerPage = 250
    }
  } else {
    itemsPerPage = 100
  }
  sp.PageInfo = &PageInfo{PageIndex: pageIndex, ItemsPerPage: itemsPerPage}

  return &sp, nil
}

func (sp *SearchParams) SetTotalPages(count int64) {
    var itemsPerPage int = sp.PageInfo.ItemsPerPage
    var pageIndex int = sp.PageInfo.PageIndex
    pageCount := count/int64(itemsPerPage)
    if count % int64(itemsPerPage) > 0 {
      pageCount += 1
    }
    sp.PageInfo = &PageInfo{PageIndex: int(pageIndex), ItemsPerPage: int(itemsPerPage), TotalItemCount: count, TotalPageCount: pageCount}
}

func DoList(w http.ResponseWriter, r *http.Request, contextJoins []*JoinData, searchFunc SearchFunc, resourceName string) {
  // note that the auth is handled by the caller because they caller may want to use the token in setting the contextJoins
  var sp *SearchParams
  var restErr RestError
  if sp, restErr = ExtractSearchParamsFromUrl(w, r); restErr != nil {
    // HTTP response is already set
    return
  }

  items, count, err := searchFunc(sp, r.Context(), contextJoins)
  if err != nil {
    rest.HandleError(w, err)
    return
  }

  sp.SetTotalPages(count)

  StandardResponse(w, items, resourceName + ` retrieved.`, sp)
}

func CommonHandler(handler func(*fireauth.ScopedClient, http.ResponseWriter, *http.Request)) (func(http.ResponseWriter, *http.Request)) {
  return func(w http.ResponseWriter, r *http.Request) {
    if fireauth, restErr := fireauth.GetClient(r); restErr != nil {
      rest.HandleError(w, restErr)
    } else {
      handler(fireauth, w, r)
      // TODO: it would be cool to verify that auth has been checked
    }
  }
}
