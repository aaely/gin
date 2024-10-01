package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	// Create a new Gin router instance
	router := gin.Default()
	router.Use(cors.Default())

	neo4jUri := "bolt://localhost:7687"
	username := "neo4j"
	password := "Asdf123$"
	driver, err := neo4j.NewDriverWithContext(neo4jUri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		panic(fmt.Sprintf("Failed to create driver: %v", err))
	}
	defer driver.Close(context.Background())

	// Handle file upload
	router.POST("/upload", func(c *gin.Context) {
		file, err := c.FormFile("file")
		if err != nil {
			c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
			return
		}

		// Ensure the upload directory exists
		uploadPath := "/home/aely/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-872ee3a1-a519-4d89-9d25-45bf60d439a1/import"
		if err := os.MkdirAll(uploadPath, os.ModePerm); err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("could not create upload directory: %s", err.Error()))
			return
		}

		// Save the uploaded file
		filePath := filepath.Join(uploadPath, file.Filename)
		if err := c.SaveUploadedFile(file, filePath); err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("upload file err: %s", err.Error()))
			return
		}

		session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		defer session.Close(context.Background())

		// Run queries using implicit transaction management in Neo4j
		// Query 1: Create the trailers and their corresponding load receipts
		query1 := `
		CALL {
		  LOAD CSV WITH HEADERS FROM 'file:///` + file.Filename + `' AS line
		  MERGE (trailer:Trailer {id: line.TrailerID})
		  MERGE (sid:SID {id: line.SID, ciscoID: line.CiscoID})
		  ON CREATE SET sid.id = line.SID
		  MERGE (trailer)-[:HAS_SID]->(sid)
		  MERGE (sid)-[:HAS_PART]->(part:Part {number: line.PartNumber, quantity: toInteger(line.Quantity)})
		  MERGE (sid)-[:BELONGS_TO]->(trailer)
		} IN TRANSACTIONS OF 100 ROWS;
		`
		_, err = session.Run(context.Background(), query1, nil)
		if err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("neo4j query err (query 1): %s", err.Error()))
			return
		}

		// Query 2: Create relations of TrailerID and CiscoIDs
		query2 := `
		CALL {
		  LOAD CSV WITH HEADERS FROM 'file:///` + file.Filename + `' AS line
		  MERGE (trailer:Trailer {id: line.TrailerID})
		  MERGE (cisco:Cisco {id: line.CiscoID})
		  MERGE (trailer)-[:HAS_CISCO]->(cisco)
		} IN TRANSACTIONS OF 100 ROWS;
		`
		_, err = session.Run(context.Background(), query2, nil)
		if err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("neo4j query err (query 2): %s", err.Error()))
			return
		}

		// Query 3: Create Schedule nodes for trailers that have no schedule node
		query3 := `
		  MATCH (trailer:Trailer)
		  WHERE NOT (trailer)-[:HAS_SCHEDULE]->(:Schedule)
		  CREATE (trailer)-[:HAS_SCHEDULE]->(S:Schedule{
			TrailerID: trailer.TrailerID,
			RequestDate: '',
			ScheduleDate: '',
			ScheduleTime: '',
			CarrierCode: '',
			ArrivalTime: '',
			DoorNumber: '',
			Email: '',
			LoadStatus: 'in-transit',
			IsHot: false
		  })
		`
		_, err = session.Run(context.Background(), query3, nil)
		if err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("neo4j query err (query 3): %s", err.Error()))
			return
		}

		// Query 4: Merge Parts with TrailerIDs
		query4 := `
		CALL {
		  MATCH (t:Trailer)-[:HAS_SID]->(sid:SID)-[:HAS_PART]->(p:Part)
		  MERGE (t)-[:CONTAINS_PART]->(p)
		} IN TRANSACTIONS OF 100 ROWS;
		`
		_, err = session.Run(context.Background(), query4, nil)
		if err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("neo4j query err (query 4): %s", err.Error()))
			return
		}
		c.String(http.StatusOK, fmt.Sprintf("File %s uploaded successfully to %s and processed in Neo4j", file.Filename, filePath))
	})

	srv := &http.Server{
		Addr:         ":8888",
		Handler:      router,
		ReadTimeout:  5 * time.Minute, // Increase read timeout
		WriteTimeout: 5 * time.Minute, // Increase write timeout
	}

	// Start the server with TLS
	/*if err := srv.ListenAndServeTLS("/usr/local/share/ca-certificates/s.crt", "/home/aely/rocket_backend/certs/s.key"); err != nil && err != http.ErrServerClosed {
		panic(fmt.Sprintf("Server error: %v", err))
	}*/
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(fmt.Sprintf("Server error: %v", err))
	}
}
