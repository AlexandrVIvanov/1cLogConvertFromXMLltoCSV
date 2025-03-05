package main

import (
	"context"
	"encoding/csv"
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type EventLog struct {
	XMLName xml.Name `xml:"EventLog"`
	Events  []Event  `xml:"Event"`
}

type Event struct {
	Level                   string `xml:"Level"`
	Date                    string `xml:"Date"`
	ApplicationName         string `xml:"ApplicationName"`
	ApplicationPresentation string `xml:"ApplicationPresentation"`
	Event                   string `xml:"Event"`
	EventPresentation       string `xml:"EventPresentation"`
	User                    string `xml:"User"`
	UserName                string `xml:"UserName"`
	Computer                string `xml:"Computer"`
	Metadata                string `xml:"Metadata"`
	MetadataPresentation    string `xml:"MetadataPresentation"`
	Comment                 string `xml:"Comment"`
	Data                    string `xml:"Data"`
	DataPresentation        string `xml:"DataPresentation"`
	TransactionStatus       string `xml:"TransactionStatus"`
	TransactionID           string `xml:"TransactionID"`
	Connection              string `xml:"Connection"`
	Session                 string `xml:"Session"`
	ServerName              string `xml:"ServerName"`
	Port                    string `xml:"Port"`
	SyncPort                string `xml:"SyncPort"`
}

func main() {
	// Определяем флаги
	xmlFileName := flag.String("f", "", "Имя XML-файла для обработки")
	dbName := flag.String("b", "", "Имя базы данных (обязательно)")
	clickhouseDSN := flag.String("ch", "tcp://localhost:9000", "DSN для подключения к ClickHouse")
	clickhouseDB := flag.String("t", "default", "Имя базы данных в ClickHouse")
	clickhouseUser := flag.String("u", "default", "Пользователь ClickHouse")
	clickhousePassword := flag.String("p", "", "Пароль ClickHouse")
	flag.Parse()

	// Проверяем, указано ли имя XML-файла и имя базы данных
	if *xmlFileName == "" || *dbName == "" {
		fmt.Println("Использование: convert1cxmltoclickhose -f <имя_файла.xml> -b <имя_базы_данных> [-ch <clickhouse_dsn>] [-t <имя_базы_clickhouse>] [-u <пользователь>] [-p <пароль>]")
		return
	}

	// Открываем XML-файл
	xmlFile, err := os.Open(*xmlFileName)
	if err != nil {
		fmt.Println("Ошибка открытия файла:", err)
		return
	}
	defer xmlFile.Close()

	// Декодируем XML-файл
	var eventLog EventLog
	decoder := xml.NewDecoder(xmlFile)
	err = decoder.Decode(&eventLog)
	if err != nil {
		fmt.Println("Ошибка декодирования XML:", err)
		return
	}

	// Создаем CSV-файл
	csvFileName := fmt.Sprintf("%s_eventlog.csv", *dbName) // Используем имя базы данных в имени CSV-файла

	csvFile, err := os.Create(csvFileName)
	if err != nil {
		fmt.Println("Ошибка создания CSV-файла:", err)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	writer.Comma = ';' // Устанавливаем разделитель на точку с запятой
	defer writer.Flush()

	// Записываем заголовки CSV
	headers := []string{
		"DatabaseName", // Новая колонка
		"Level", "Date", "ApplicationName", "ApplicationPresentation", "Event",
		"EventPresentation", "User", "UserName", "Computer", "Metadata",
		"MetadataPresentation", "Comment", "Data", "DataPresentation",
		"TransactionStatus", "TransactionID", "Connection", "Session",
		"ServerName", "Port", "SyncPort",
	}
	writer.Write(headers)

	// Записываем данные в CSV
	for _, event := range eventLog.Events {
		record := []string{
			*dbName, // Значение из флага -b
			event.Level,
			event.Date,
			event.ApplicationName,
			event.ApplicationPresentation,
			event.Event,
			event.EventPresentation,
			event.User,
			event.UserName,
			event.Computer,
			event.Metadata,
			event.MetadataPresentation,
			event.Comment,
			event.Data,
			event.DataPresentation,
			event.TransactionStatus,
			event.TransactionID,
			event.Connection,
			event.Session,
			event.ServerName,
			event.Port,
			event.SyncPort,
		}
		writer.Write(record)
	}

	fmt.Printf("Данные успешно записаны в файл %s с разделителем ';'\n", csvFileName)

	// Подключение к ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*clickhouseDSN},
		Auth: clickhouse.Auth{
			Database: *clickhouseDB,
			Username: *clickhouseUser,
			Password: *clickhousePassword,
		},
	})
	if err != nil {
		fmt.Println("Ошибка подключения к ClickHouse:", err)
		return
	}
	defer conn.Close()

	// Создаем таблицу в ClickHouse (если она не существует)
	tableName := fmt.Sprintf("%s_events", *dbName) // Используем имя базы данных для имени таблицы

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			DatabaseName String,
			Level String,
			Date DateTime,
			ApplicationName String,
			ApplicationPresentation String,
			Event String,
			EventPresentation String,
			User String,
			UserName String,
			Computer String,
			Metadata String,
			MetadataPresentation String,
			Comment String,
			Data String,
			DataPresentation String,
			TransactionStatus String,
			TransactionID String,
			Connection String,
			Session String,
			ServerName String,
			Port UInt16,
			SyncPort UInt16
		) ENGINE = MergeTree()
		ORDER BY Date;
	`, tableName)

	// Используем контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Выполняем запрос на создание таблицы
	err = conn.Exec(ctx, query)
	if err != nil {
		fmt.Println("Ошибка создания таблицы в ClickHouse:", err)
		return
	}

	// Загружаем данные из CSV в ClickHouse
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			DatabaseName, Level, Date, ApplicationName, ApplicationPresentation, Event,
			EventPresentation, User, UserName, Computer, Metadata,
			MetadataPresentation, Comment, Data, DataPresentation,
			TransactionStatus, TransactionID, Connection, Session,
			ServerName, Port, SyncPort
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tableName)

	// Читаем CSV-файл и вставляем данные в ClickHouse
	file, err := os.Open(csvFileName)
	if err != nil {
		fmt.Println("Ошибка открытия CSV-файла:", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Ошибка чтения CSV-файла:", err)
		return
	}

	batch, err := conn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		fmt.Println("Ошибка подготовки пакетной вставки:", err)
		return
	}

	for i, record := range records[1:] { // Пропускаем заголовок
		err = batch.Append(
			record[0],  // DatabaseName
			record[1],  // Level
			record[2],  // Date
			record[3],  // ApplicationName
			record[4],  // ApplicationPresentation
			record[5],  // Event
			record[6],  // EventPresentation
			record[7],  // User
			record[8],  // UserName
			record[9],  // Computer
			record[10], // Metadata
			record[11], // MetadataPresentation
			record[12], // Comment
			record[13], // Data
			record[14], // DataPresentation
			record[15], // TransactionStatus
			record[16], // TransactionID
			record[17], // Connection
			record[18], // Session
			record[19], // ServerName
			record[20], // Port
			record[21], // SyncPort
		)
		if err != nil {
			fmt.Printf("Ошибка добавления строки %d в пакет: %v\n", i+1, err)
			return
		}
	}

	// Выполняем пакетную вставку
	err = batch.Send()
	if err != nil {
		fmt.Println("Ошибка выполнения пакетной вставки:", err)
		return
	}

	fmt.Printf("Данные успешно загружены в таблицу %s в ClickHouse\n", tableName)
}
