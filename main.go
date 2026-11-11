package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

//
// 1. Структуры данных
//

// Job - задание для воркера
type Job struct {
	ID  int
	URL string
}

// Result - результат обработки задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

//
// 2. Функция-воркер
//

func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		// Имитация HTTP-запроса со случайной задержкой
		randomDuration := time.Duration(rand.Intn(500)+100) * time.Millisecond
		time.Sleep(randomDuration)

		// Отправка результата в канал
		results <- Result{
			Job:      job,
			Status:   "Обработан",
			Duration: randomDuration,
		}
	}
}

//
// 3-5
//

func main() {
	// Список URL-адресов
	urls := []string{
		"https://example.com",
		"https://google.com",
		"https://github.com",
		"https://stackoverflow.com",
		"https://reddit.com",
		"https://youtube.com",
		"https://twitter.com",
		"https://facebook.com",
		"https://linkedin.com",
		"https://amazon.com",
	}

	// Создаём каналы
	jobs := make(chan Job, len(urls))       // Буферизированный канал для заданий
	results := make(chan Result, len(urls)) // Буферизированный канал для результатов

	// Количество воркеров
	numWorkers := 5
	var wg sync.WaitGroup

	// Запускаем воркеры
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// Отправляем задания в канал jobs
	for i, url := range urls {
		jobs <- Job{ID: i + 1, URL: url}
	}
	close(jobs) // Закрываем канал

	// Отдельная горутина ждёт завершения всех воркеров и закрывает results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Собираем результаты
	var allResults []Result
	for result := range results {
		allResults = append(allResults, result)
	}

	//
	// 5. Вывод отчёта
	//

	fmt.Println("=== Отчёт по обработке URL ===")
	fmt.Printf("Всего обработано: %d URL\n\n", len(allResults))

	var totalDuration time.Duration
	var successCount int

	for _, result := range allResults {
		fmt.Printf("[%d] %s — %s (%v)\n",
			result.Job.ID,
			result.Job.URL,
			result.Status,
			result.Duration,
		)
		totalDuration += result.Duration
		if result.Status == "Обработан" {
			successCount++
		}
	}

	fmt.Println("\n=== Статистика ===")
	fmt.Printf("Успешных операций: %d / %d\n", successCount, len(allResults))
	fmt.Printf("Общее время выполнения: %v\n", totalDuration)
	fmt.Printf("Среднее время на запрос: %v\n", totalDuration/time.Duration(len(allResults)))
}
