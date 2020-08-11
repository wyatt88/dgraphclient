package dgraphclient

import (
	"encoding/json"
	"fmt"
	"time"

	// 有版本的区分
	"context"
	dgo "github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"log"
)

type DgraphClient interface {
	newClient()
}

type Client struct {
	Hostname string
	Port     int
}

func (c Client) Setup(schema string) error {
	return c.setup(schema)
}
func (c Client) setup(schema string) error {
	client := c.newClient()
	err := client.Alter(context.Background(), &api.Operation{
		Schema: schema,
	})
	return err
}

func (c Client) newClient() *dgo.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	dialOpts := append([]grpc.DialOption{},
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	dgraphUrl := fmt.Sprintf("%s:%d", c.Hostname, c.Port)
	d, err := grpc.Dial(dgraphUrl, dialOpts...)
	if err != nil {
		log.Fatal(err)
	}
	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func (c Client) Insert(object interface{}) (map[string]string, error) {
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
	return assigned.Uids, err
}

func (c Client) Query(q string) (*api.Response, error) {
	return c.query(q)
}

func (c Client) query(q string) (*api.Response, error) {
	client := c.newClient()
	txn := client.NewTxn()
	resp, err := txn.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c Client) IsExisted(key string, value interface{}) bool {
	queryBaseString := `
{
	all(func: eq(%s,%v)) {
		uid
	}
}
`
	queryString := fmt.Sprintf(queryBaseString, key, value)
	resp, err := c.query(queryString)
	if err != nil {
		log.Print(err)
		return false
	}
	var decode struct{
		All []struct{
			Uid string
		}
	}
	if err := json.Unmarshal(resp.GetJson(), &decode); err != nil {
		log.Print(err)
		return false
	}
	if len(decode.All) <= 0 {
		return false
	}
	return true
}

func (c Client)Update(object interface{}) (bool,error) {
	return c.update(object)
}

func (c Client)update(object interface{}) (bool,error)  {
	client := c.newClient()
	ctx := context.Background()
	mu := &api.Mutation{
		CommitNow: true,
	}
	pb, err := json.Marshal(object)
	if err != nil {
		return false, err
	}
	mu.SetJson = pb
	_, err = client.NewTxn().Mutate(ctx, mu)
	if err != nil {
		return false, err
	}
	return true, err
}

func (c Client) DropAll() {
	client := c.newClient()
	ctx, toCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer toCancel()
	if err := client.Alter(ctx, &api.Operation{DropAll: true}); err != nil {
		log.Fatal("The drop all operation should have succeeded")
	}
}
