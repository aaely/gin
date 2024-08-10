package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

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
		uploadPath := "/var/local/"
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

		_, err = session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
			// Create the trailers and their corresponding load receipts
			query1 := `
            LOAD CSV WITH HEADERS FROM 'file:///` + file.Filename + `' AS line
            MERGE (trailer.Trailer {id: line.TrailerID})
			MERGE (sid: SID {id: line.SID, ciscoID: line.CiscoID})
			ON CREATE SET sid.id = line.SID
			MERGE (trailer)-[:HAS_SID]->(sid)
			MERGE (sid)-[:HAS_PART]->(part:Part {number: line.PartNumber, quantity: toInteger(line.Quantity)})
			MERGE (sid)-[:BELONGS_TO]->(trailer)
            `
			_, err := tx.Run(context.Background(), query1, nil)
			if err != nil {
				return nil, err
			}

			// Create relations of TrailerID and CiscoIDs
			query2 := `
            LOAD CSV WITH HEADERS FROM 'file:///` + file.Filename + `' AS line
            MERGE (trailer:Trailer {id: line.TrailerID})
			MERGE (cisco:Cisco {id: line.CiscoID})
			MERGE (trailer)-[:HAS_CISCO]->(cisco)
            `
			_, err = tx.Run(context.Background(), query2, nil)
			if err != nil {
				return nil, err
			}

			// Create Schedule nodes for trailers that have no schedule node
			query3 := `
            LOAD CSV WITH HEADERS FROM 'file:///` + file.Filename + `' AS line
			MATCH (trailer:Trailer)
			WHERE NOT (trailer)-[:HAS_SCHEDULE]->(:Schedule)
			CREATE (trailer)-[:HAS_SCHEDULE]->(S:Schedule{
			TrailerID: trailer.TrailerID,
			RequestDate: '',
			ScheduleDate: '',
			ScheduleTime: '',
			CarrierCode: '',
			CarrierCode: '',
			ArrivalTime: '',
			DoorNumber: '',
			Email: '',
			LoadStatus: 'in-transit',
			IsHot: false
			})
            `

			_, err = tx.Run(context.Background(), query3, nil)
			if err != nil {
				return nil, err
			}

			// Merge Parts with TrailerIDs
			query4 := `
            MATCH (t:Trailer)-[:HAS_SID]->(sid:SID)-[:HAS_PART]->(p:Part)
            ...
            `

			_, err = tx.Run(context.Background(), query4, nil)
			return nil, err
		})

		if err != nil {
			c.String(http.StatusInternalServerError, fmt.Sprintf("neo4j query err: %s", err.Error()))
			return
		}

		c.String(http.StatusOK, fmt.Sprintf("File %s uploaded successfully to %s and processed in Neo4j", file.Filename, filePath))
	})

	// Start the server on port 8080
	router.Run(":8888")
	//router.RunTLS(":8888", "/usr/local/share/ca-certificates/s.crt", "/home/aely/rocket_backend/certs/s.key")
}
