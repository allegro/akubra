package privacy

import (
	"context"
	"fmt"

	"net/http"

	"github.com/allegro/akubra/internal/akubra/log"
)

const (
	//RequestPrivacyContext is the key under which PrivacyContext can be found in context.Context
	RequestPrivacyContext = log.ContextKey("RequestPrivacyContext")
)

//Context holds the privacy settings associated with the request
type Context struct {
	isInternalNetwork bool
}

//ContextSupplier supplies the context.Context of req with a privacy context
type ContextSupplier interface {
	Supply(req *http.Request) (*http.Request, error)
}

//BasicPrivacyContextSupplier is a basic implemtation of ContextSupplier
type BasicPrivacyContextSupplier struct {
	isInternalNetworkHeaderName  string
	isInternalNetworkHeaderValue string
}

//NewBasicPrivacyContextSupplier creates an instance of BasicPrivacyContextSupplier
func NewBasicPrivacyContextSupplier(isInternalNetworkHeaderName, isInternalNetworkHeaderValue string) ContextSupplier {
	return &BasicPrivacyContextSupplier{
		isInternalNetworkHeaderName:  isInternalNetworkHeaderName,
		isInternalNetworkHeaderValue: isInternalNetworkHeaderValue,
	}
}

//Supply supplies the request with basic privacy info
func (basicSupplier *BasicPrivacyContextSupplier) Supply(req *http.Request) (*http.Request, error) {
	isInternalNetwork := req.Header.Get(basicSupplier.isInternalNetworkHeaderName) == basicSupplier.isInternalNetworkHeaderValue
	privacyContext := &Context{
		isInternalNetwork: isInternalNetwork,
	}
	contextWithPrivacy := context.WithValue(req.Context(), RequestPrivacyContext, privacyContext)
	return req.WithContext(contextWithPrivacy), nil
}

//SupplierRoundTripper supplies the request with a privacy context using the given Supplier
type SupplierRoundTripper struct {
	roundTripper           http.RoundTripper
	privacyContextSupplier ContextSupplier
}

//NewSupplierRoundTripper creates an instance of SupplierRoundTripper
func NewSupplierRoundTripper(roundTripper http.RoundTripper, supplier ContextSupplier) http.RoundTripper {
	return &SupplierRoundTripper{
		roundTripper:           roundTripper,
		privacyContextSupplier: supplier,
	}
}

//RoundTrip supplies the request with basic privacy info
func (supplierRT *SupplierRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	reqWithPrivacy, err := supplierRT.privacyContextSupplier.Supply(req)
	if err != nil {
		reqID := req.Context().Value(log.ContextreqIDKey)
		return nil, fmt.Errorf("failed to supply request %s with privacy context, reason: %s", reqID, err)
	}
	return supplierRT.roundTripper.RoundTrip(reqWithPrivacy)
}
