package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/tubar2/gRPC_and_MongoDB/blog/blogpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var collection *mongo.Collection

type blogItem struct {
	ID       primitive.ObjectID `bson:"_id,omitempty"`
	AuthorId string             `bson:"title,omitempty"`
	Content  string             `bson:"author,omitempty"`
	Title    string             `bson:"tags,omitempty"`
}

func (data *blogItem) ToBlogPb() *blogpb.Blog {
	return &blogpb.Blog{
		Id:       data.ID.Hex(),
		AuthorId: data.AuthorId,
		Content:  data.Content,
		Title:    data.Title,
	}
}

type server struct {
	blogpb.UnimplementedBlogServiceServer
}

func (server *server) CreateBlog(ctx context.Context, req *blogpb.CreateBlogRequest) (*blogpb.CreateBlogResponse, error) {
	log.Println("Creating blog")
	blog := req.GetBlog()

	blogItem := blogItem{
		AuthorId: blog.GetAuthorId(),
		Content:  blog.GetContent(),
		Title:    blog.GetTitle(),
	}

	res, err := collection.InsertOne(context.Background(), blogItem)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintln("Internal error:", err),
		)
	}

	oid, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintln("Cannot convert to primitive ID", err),
		)
	}
	log.Println("Blog created")
	return &blogpb.CreateBlogResponse{
		Blog: &blogpb.Blog{
			Id:       oid.Hex(),
			AuthorId: blog.GetAuthorId(),
			Title:    blog.GetTitle(),
			Content:  blog.GetContent(),
		},
	}, nil
}

func (server *server) ReadBlog(ctx context.Context, req *blogpb.ReadBlogRequest) (*blogpb.ReadBlogResponse, error) {
	blogId := req.GetBlogId()
	log.Println("Trying to read blog:", blogId)

	oid, err := primitive.ObjectIDFromHex(blogId)
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintln("Cannot parse ID", err),
		)
	}

	data := blogItem{}
	filter := bson.D{
		primitive.E{
			Key:   "_id",
			Value: oid,
		},
	}

	if err := collection.FindOne(context.Background(), filter).Decode(&data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintln("Cannot find blog from id", err),
		)
	}

	return &blogpb.ReadBlogResponse{
		Blog: &blogpb.Blog{
			Id:       data.ID.Hex(),
			AuthorId: data.AuthorId,
			Content:  data.Content,
			Title:    data.Title,
		},
	}, nil
}

func (server *server) UpdateBlog(ctx context.Context, req *blogpb.UpdateBlogRequest) (*blogpb.UpdateBlogResponse, error) {
	log.Println("Updating blog")
	blog := req.GetBlog()
	oid, err := primitive.ObjectIDFromHex(blog.GetId())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintln("Cannot parse ID", err),
		)
	}
	data := blogItem{}
	filter := bson.D{
		primitive.E{
			Key:   "_id",
			Value: oid,
		},
	}

	if err := collection.FindOne(context.Background(), filter).Decode(&data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintln("Cannot find blog from id", err),
		)
	}

	data.AuthorId = blog.GetAuthorId()
	data.Content = blog.GetContent()
	data.Title = blog.GetTitle()

	updateResult, err := collection.ReplaceOne(context.Background(), filter, data)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintln("Cannot update object in mongoDB", err),
		)
	}
	log.Println("Succesfully updated record", updateResult)

	return &blogpb.UpdateBlogResponse{
		Blog: data.ToBlogPb(),
	}, nil
}

func (server *server) DeleteBlog(ctx context.Context, req *blogpb.DeleteBlogRequest) (*blogpb.DeleteBlogResponse, error) {
	blog_id := req.GetBlogId()
	log.Println("Deleting blog", blog_id)

	oid, err := primitive.ObjectIDFromHex(blog_id)
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintln("Cannot parse ID", err),
		)
	}

	// data := blogItem{}
	filter := bson.D{
		primitive.E{
			Key:   "_id",
			Value: oid,
		},
	}
	res, err := collection.DeleteOne(context.Background(), filter)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintln("Cannot Delete object from mongoDB", err),
		)
	}
	if res.DeletedCount == 0 {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintln("Couldn't find object in mongoDB", err),
		)
	}

	return &blogpb.DeleteBlogResponse{
		Result: fmt.Sprintln(blog_id),
	}, nil
}

func (server *server) ListBlog(_ *blogpb.ListBlogRequest, stream blogpb.BlogService_ListBlogServer) error {
	log.Println("Listing all blog entrys")
	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()

	cursor, err := collection.Find(context.Background(), bson.D{})
	defer cursor.Close(context.Background())
	if err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintln("Internal error retrieving all elements", err),
		)
	}

	for cursor.Next(context.Background()) {
		data := blogItem{}
		err := cursor.Decode(&data)
		if err != nil {
			return status.Errorf(
				codes.Internal,
				fmt.Sprintln("Error decoding data", err),
			)
		}
		if err = stream.Send(
			&blogpb.ListBlogResponse{
				Blog: data.ToBlogPb(),
			},
		); err != nil {
			return status.Errorf(
				codes.Unknown,
				fmt.Sprintln("Error sending data over the stream", err),
			)
		}
	}
	if err = cursor.Err(); err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintln("Error iterating over data", err),
		)
	}

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("Creating Listener")
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalln("Failed to listen", err)
	}

	log.Println("Registering new server")

	opts := []grpc.ServerOption{}
	s := grpc.NewServer(opts...)
	blogpb.RegisterBlogServiceServer(s, &server{})

	go func() {
		log.Println("Starting Server")
		if err := s.Serve(lis); err != nil {
			log.Fatalln("Failed to server", err)
		}
	}()

	log.Println("Establishing connection to mongodb databse")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalln("Error connecting to mongoDB", err)
	}
	log.Println("Connection to mongodb succesful")

	defer func() {
		log.Println("Disconnecting client from mongodb")
		if err := client.Disconnect(ctx); err != nil {
			log.Panic("Client Disconnected")
		}
	}()

	ping(client, ctx)

	collection = client.Database("mydb").Collection("blog")

	// Wait for control C to exit
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	<-ch
	log.Println("Stopping Server")
	s.Stop()
	lis.Close()

}

func ping(client *mongo.Client, ctx context.Context) {
	log.Println("Pingning")
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		log.Panic("Error Pinging client", err)
	}
	log.Println("Pinging succesfull")
}
