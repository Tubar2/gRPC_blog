package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/tubar2/gRPC_and_MongoDB/blog/blogpb"
	"google.golang.org/grpc"
)

func main() {
	log.Println("Starting Blog_client")

	opts := grpc.WithInsecure()

	cc, err := grpc.Dial("localhost:50051", opts)
	if err != nil {
		log.Fatalln("Client couldn't connect", err)
	}
	defer cc.Close()

	c := blogpb.NewBlogServiceClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := unaryCreateBlog(ctx, c); err != nil {
		log.Println("Couldn't create blog", err)
	}

	if err = unaryReadBlog(ctx, c, "60ceb64684340fc393c74837"); err != nil {
		log.Println("Couldn't read blog", err)
	}
	if err = unaryUpdateBlog(ctx, c, "60cf55894053d8e1c9438bde"); err != nil {
		log.Println("Couldn't update blog", err)
	}
	if err = unaryReadBlog(ctx, c, "60cf55894053d8e1c9438bde"); err != nil {
		log.Println("Couldn't read blog", err)
	}
	if err = unaryDeleteBlog(ctx, c, "60cebe00238f794eb19b68ff"); err != nil {
		log.Println("Couldn't read blog", err)
	}
	if err = listBlogItems(ctx, c); err != nil {
		log.Println("Couldn't read blog", err)
	}
}

func unaryCreateBlog(ctx context.Context, c blogpb.BlogServiceClient) error {
	log.Println("Creating blog")
	blog := &blogpb.Blog{
		AuthorId: "Ricardo",
		Title:    "My first Blog",
		Content:  "This is my frist blog, wohooo",
	}

	createBlogResponse, err := c.CreateBlog(ctx, &blogpb.CreateBlogRequest{
		Blog: blog,
	})
	if err != nil {
		return err
	}
	fmt.Println("Created blog:", createBlogResponse)
	return nil
}

func unaryReadBlog(ctx context.Context, c blogpb.BlogServiceClient, blogID string) error {
	log.Println("Reading blog")

	res, err := c.ReadBlog(ctx, &blogpb.ReadBlogRequest{
		BlogId: blogID,
	})
	if err != nil {
		return err
	}

	fmt.Println("Read blog is:", res.GetBlog())
	return nil
}

func unaryUpdateBlog(ctx context.Context, c blogpb.BlogServiceClient, blogID string) error {
	log.Println("Updating blog")

	blog := &blogpb.Blog{
		Id:       blogID,
		AuthorId: "Ricardo",
		Title:    "My first Blog (updated)",
		Content:  "This is my frist blog, wohooo, now with more features",
	}

	updateRes, err := c.UpdateBlog(ctx, &blogpb.UpdateBlogRequest{
		Blog: blog,
	})
	if err != nil {
		return err
	}

	fmt.Println("Blog updated!", updateRes)

	return nil
}

func unaryDeleteBlog(ctx context.Context, c blogpb.BlogServiceClient, blogID string) error {
	log.Println("Deleting blog", blogID)

	res, err := c.DeleteBlog(ctx, &blogpb.DeleteBlogRequest{
		BlogId: blogID,
	})
	if err != nil {
		return err
	}
	fmt.Println("Deleted Blog:", res.Result)

	return nil
}

func listBlogItems(ctx context.Context, c blogpb.BlogServiceClient) error {
	log.Println("Lisitng all blogs")

	stream, err := c.ListBlog(ctx, &blogpb.ListBlogRequest{})
	if err != nil {
		return err
	}

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println("BlogItem:", res.GetBlog())
	}
	return nil
}
