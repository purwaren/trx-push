package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"bytes"
	"io"
	"os"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

type Config struct {
	API struct {
		LoginURL string `yaml:"login_url"`
		PushURL  string `yaml:"push_url"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"api"`
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		DBName   string `yaml:"dbname"`
		SSLMode  string `yaml:"sslmode"`
	} `yaml:"database"`
}

type Transaction struct {
	InvoiceID string `json:"invoice_id"`
}

type LoginResponse struct {
	Token string `json:"access_token"`
}

var (
	config   Config
	jwtToken string
)

func main() {
	// Step 1: Load configuration
	if err := loadConfig(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Step 2: Acquire JWT token
	if err := loginAndGetToken(); err != nil {
		log.Fatalf("Failed to login and get JWT token: %v", err)
	}

	// Step 3: Connect to the database and retrieve transactions
	transactions, err := getTransactionsFromDB()
	if err != nil {
		log.Fatalf("Failed to get transactions from the database: %v", err)
	}

	// Step 4: Push transactions
	for _, txn := range transactions {
		if err := pushTransaction(txn.InvoiceID); err != nil {
			log.Printf("Failed to push transaction with invoice_id %s: %v", txn.InvoiceID, err)
		} else {
			log.Printf("Successfully pushed transaction with invoice_id %s", txn.InvoiceID)
		}
	}
}

// Load configuration from YAML file
func loadConfig() error {
	file, err := os.Open("config.yaml")
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return err
	}
	return nil
}

// Get transactions with status = 1 from the PostgreSQL database
func getTransactionsFromDB() ([]Transaction, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Database.Host, config.Database.Port, config.Database.User, config.Database.Password, config.Database.DBName, config.Database.SSLMode)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SELECT number FROM invoice WHERE status = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []Transaction
	for rows.Next() {
		var number string
		if err := rows.Scan(&number); err != nil {
			return nil, err
		}
		transactions = append(transactions, Transaction{InvoiceID: number})
	}

	return transactions, nil
}

// Push a transaction by invoice_id
func pushTransaction(invoiceID string) error {
	url := fmt.Sprintf("%s?invoice_number=%s", config.API.PushURL, invoiceID)
	fmt.Printf("URL push: %s\n", url)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to push transaction with invoice_id %s, status: %d", invoiceID, resp.StatusCode)
	}
	return nil
}

// Login and get JWT token
func loginAndGetToken() error {
	loginData := map[string]string{
		"email": config.API.Username,
		"password": config.API.Password,
	}
	jsonData, _ := json.Marshal(loginData)

	req, err := http.NewRequest("POST", config.API.LoginURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	//fmt.Printf("resp: %s", string(body));

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to login, status: %d", resp.StatusCode)
	}

	var loginResp LoginResponse
	if err := json.Unmarshal(body, &loginResp); err != nil {
		return err
	}

	jwtToken = loginResp.Token
	log.Printf("Successfully acquired JWT token: %s", jwtToken)
	return nil
}