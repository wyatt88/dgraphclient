package dgraphclient

import (
	"encoding/json"
	"fmt"
	"time"

	// 有版本的区分
	dgo "github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"log"
	"context"
)

type DgraphClient interface {
	newClient()
}

type Client struct {
	Hostname string
	Port int
}

func (c Client)newClient() *dgo.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	dialOpts := append([]grpc.DialOption{},
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	dgraphUrl := fmt.Sprintf("%s:%d",c.Hostname,c.Port)
	d, err := grpc.Dial(dgraphUrl, dialOpts...)
	if err != nil {
		log.Fatal(err)
	}
	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func (c Client)Insert(object interface{}) (map[string]string,error) {
	client := c.newClient()
	ctx := context.Background()
	mu := &api.Mutation{
		CommitNow: true,
	}
	pb, err := json.Marshal(object)
	if err != nil {
		log.Fatal(err)
	}
	mu.SetJson = pb
	assigned, err := client.NewTxn().Mutate(ctx, mu)
	if err != nil {
		log.Fatal(err)
	}
	return assigned.Uids,err
}

func (c Client)DropAll()  {
	client := c.newClient()
	ctx, toCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer toCancel()
	if err := client.Alter(ctx, &api.Operation{DropAll: true}); err != nil {
		log.Fatal("The drop all operation should have succeeded")
	}
}

