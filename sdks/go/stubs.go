package crabka

import "context"

type Database struct{ client *Client }

func (d *Database) Connect(context.Context, string) error {
	return gatedError("database", "chapter-f-control-plane")
}

type Auth struct{ client *Client }

func (a *Auth) BearerToken() string {
	return a.client.bearerToken
}

func (a *Auth) SignIn(context.Context, string, string) error {
	return errorWithMessage(Unauthenticated, "identity APIs are not part of contract v1")
}

type Blob struct{ client *Client }

func (b *Blob) Put(context.Context, string, []byte) error {
	return gatedError("blob", "chapter-b-blob-api")
}

func (b *Blob) Get(context.Context, string) ([]byte, error) {
	return nil, gatedError("blob", "chapter-b-blob-api")
}
