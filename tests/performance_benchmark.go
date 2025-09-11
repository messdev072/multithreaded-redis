package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceTest runs comprehensive benchmarks against the optimized Redis server
func main() {
	fmt.Println("Redis Performance Optimization Test Suite")
	fmt.Println("Testing: Lock contention, buffered I/O, RESP parsing, GC pressure reduction")
	fmt.Println(strings.Repeat("=", 80))

	// Test scenarios
	tests := []struct {
		name     string
		clients  int
		requests int
		testFunc func(clients, requests int) TestResult
	}{
		{"Single Client Baseline", 1, 10000, testBasicOperations},
		{"Lock Contention Test (8 clients)", 8, 5000, testLockContention},
		{"High Concurrency Test (50 clients)", 50, 1000, testHighConcurrency},
		{"Buffer I/O Stress Test", 20, 2000, testBufferedIO},
		{"RESP Parsing Performance", 10, 5000, testRESPParsing},
		{"Mixed Workload Test", 25, 2000, testMixedWorkload},
	}

	for _, test := range tests {
		fmt.Printf("\n[%s]\n", test.name)
		fmt.Printf("Clients: %d, Requests per client: %d\n", test.clients, test.requests)

		result := test.testFunc(test.clients, test.requests)

		fmt.Printf("Results:\n")
		fmt.Printf("  Total Requests: %d\n", result.TotalRequests)
		fmt.Printf("  Successful: %d\n", result.Successful)
		fmt.Printf("  Failed: %d\n", result.Failed)
		fmt.Printf("  Total Time: %v\n", result.Duration)
		fmt.Printf("  Requests/sec: %.2f\n", result.RequestsPerSecond)
		fmt.Printf("  Avg Latency: %v\n", result.AvgLatency)
		fmt.Printf("  95th Percentile: %v\n", result.P95Latency)
		fmt.Printf("  99th Percentile: %v\n", result.P99Latency)
		fmt.Printf("  Success Rate: %.2f%%\n", result.SuccessRate)

		if result.SuccessRate < 99.0 {
			fmt.Printf("  ⚠️  WARNING: Low success rate detected!\n")
		} else {
			fmt.Printf("  ✅ PASS: High success rate maintained\n")
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("Performance test suite completed")
}

type TestResult struct {
	TotalRequests     int
	Successful        int64
	Failed            int64
	Duration          time.Duration
	RequestsPerSecond float64
	AvgLatency        time.Duration
	P95Latency        time.Duration
	P99Latency        time.Duration
	SuccessRate       float64
	Latencies         []time.Duration
}

// testBasicOperations tests basic SET/GET operations
func testBasicOperations(clients, requests int) TestResult {
	return runTest(clients, requests, func(conn net.Conn, clientID, reqID int) error {
		key := fmt.Sprintf("key_%d_%d", clientID, reqID)
		value := fmt.Sprintf("value_%d_%d", clientID, reqID)

		// SET operation
		setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
		if _, err := conn.Write([]byte(setCmd)); err != nil {
			return err
		}

		// Read response
		reader := bufio.NewReader(conn)
		_, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		// GET operation
		getCmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
		if _, err := conn.Write([]byte(getCmd)); err != nil {
			return err
		}

		// Read response
		_, err = reader.ReadString('\n')
		return err
	})
}

// testLockContention tests operations that stress the sharded lock system
func testLockContention(clients, requests int) TestResult {
	return runTest(clients, requests, func(conn net.Conn, clientID, reqID int) error {
		// Use overlapping key ranges to test lock contention
		keyNum := reqID % 100 // Only 100 different keys across all clients
		key := fmt.Sprintf("shared_key_%d", keyNum)
		value := fmt.Sprintf("client_%d_req_%d", clientID, reqID)

		// SET operation
		setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
		if _, err := conn.Write([]byte(setCmd)); err != nil {
			return err
		}

		reader := bufio.NewReader(conn)
		_, err := reader.ReadString('\n')
		return err
	})
}

// testHighConcurrency tests many concurrent connections
func testHighConcurrency(clients, requests int) TestResult {
	return runTest(clients, requests, func(conn net.Conn, clientID, reqID int) error {
		key := fmt.Sprintf("concurrent_%d_%d", clientID, reqID)

		// Multiple operations in sequence
		operations := []string{
			fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nhello\r\n", len(key), key),
			fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key),
			fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key),
		}

		reader := bufio.NewReader(conn)
		for _, op := range operations {
			if _, err := conn.Write([]byte(op)); err != nil {
				return err
			}
			if _, err := reader.ReadString('\n'); err != nil {
				return err
			}
		}
		return nil
	})
}

// testBufferedIO tests operations that stress the buffered I/O system
func testBufferedIO(clients, requests int) TestResult {
	return runTest(clients, requests, func(conn net.Conn, clientID, reqID int) error {
		// Send multiple small commands rapidly to test buffering
		reader := bufio.NewReader(conn)

		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("buf_%d_%d_%d", clientID, reqID, i)
			cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nx\r\n", len(key), key)

			if _, err := conn.Write([]byte(cmd)); err != nil {
				return err
			}
		}

		// Read all responses
		for i := 0; i < 5; i++ {
			if _, err := reader.ReadString('\n'); err != nil {
				return err
			}
		}
		return nil
	})
}

// testRESPParsing tests complex RESP parsing scenarios
func testRESPParsing(clients, requests int) TestResult {
	return runTest(clients, requests, func(conn net.Conn, clientID, reqID int) error {
		// Test different RESP types and complex arrays
		commands := []string{
			// Simple SET
			"*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n",
			// MSET with multiple key-value pairs
			"*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n",
			// KEYS command
			"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n",
		}

		reader := bufio.NewReader(conn)
		for _, cmd := range commands {
			if _, err := conn.Write([]byte(cmd)); err != nil {
				return err
			}
			// Read response (may be multi-line for arrays)
			if _, err := reader.ReadString('\n'); err != nil {
				return err
			}
		}
		return nil
	})
}

// testMixedWorkload tests a realistic mixed workload
func testMixedWorkload(clients, requests int) TestResult {
	return runTest(clients, requests, func(conn net.Conn, clientID, reqID int) error {
		reader := bufio.NewReader(conn)

		// Simulate realistic usage patterns
		operations := []struct {
			cmd    string
			weight int // relative frequency
		}{
			{"SET", 30},
			{"GET", 50},
			{"DEL", 10},
			{"KEYS", 5},
			{"EXISTS", 5},
		}

		// Choose operation based on request ID
		opIndex := reqID % 100
		var selectedOp string

		cumulative := 0
		for _, op := range operations {
			cumulative += op.weight
			if opIndex < cumulative {
				selectedOp = op.cmd
				break
			}
		}

		key := fmt.Sprintf("mixed_%d_%d", clientID, reqID)
		var cmd string

		switch selectedOp {
		case "SET":
			cmd = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key)
		case "GET":
			cmd = fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
		case "DEL":
			cmd = fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
		case "KEYS":
			cmd = "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n"
		case "EXISTS":
			cmd = fmt.Sprintf("*2\r\n$6\r\nEXISTS\r\n$%d\r\n%s\r\n", len(key), key)
		}

		if _, err := conn.Write([]byte(cmd)); err != nil {
			return err
		}

		_, err := reader.ReadString('\n')
		return err
	})
}

// runTest executes a test with the given parameters
func runTest(clients, requests int, testFunc func(net.Conn, int, int) error) TestResult {
	var wg sync.WaitGroup
	var successful, failed int64
	latencies := make([]time.Duration, 0, clients*requests)
	var latencyMutex sync.Mutex

	startTime := time.Now()

	// Launch client goroutines
	for clientID := 0; clientID < clients; clientID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Connect to Redis server
			conn, err := net.Dial("tcp", "localhost:6380")
			if err != nil {
				log.Printf("Failed to connect: %v", err)
				atomic.AddInt64(&failed, int64(requests))
				return
			}
			defer conn.Close()

			// Execute requests for this client
			for reqID := 0; reqID < requests; reqID++ {
				reqStart := time.Now()

				err := testFunc(conn, id, reqID)

				reqLatency := time.Since(reqStart)

				if err != nil {
					atomic.AddInt64(&failed, 1)
					log.Printf("Request failed: %v", err)
				} else {
					atomic.AddInt64(&successful, 1)
				}

				// Record latency
				latencyMutex.Lock()
				latencies = append(latencies, reqLatency)
				latencyMutex.Unlock()
			}
		}(clientID)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Calculate statistics
	totalRequests := clients * requests
	successfulCount := atomic.LoadInt64(&successful)
	failedCount := atomic.LoadInt64(&failed)

	// Sort latencies for percentile calculation
	if len(latencies) > 0 {
		// Simple bubble sort for small datasets
		for i := 0; i < len(latencies); i++ {
			for j := i + 1; j < len(latencies); j++ {
				if latencies[i] > latencies[j] {
					latencies[i], latencies[j] = latencies[j], latencies[i]
				}
			}
		}
	}

	var avgLatency, p95Latency, p99Latency time.Duration
	if len(latencies) > 0 {
		var total time.Duration
		for _, lat := range latencies {
			total += lat
		}
		avgLatency = total / time.Duration(len(latencies))

		p95Index := int(float64(len(latencies)) * 0.95)
		p99Index := int(float64(len(latencies)) * 0.99)

		if p95Index >= len(latencies) {
			p95Index = len(latencies) - 1
		}
		if p99Index >= len(latencies) {
			p99Index = len(latencies) - 1
		}

		p95Latency = latencies[p95Index]
		p99Latency = latencies[p99Index]
	}

	requestsPerSecond := float64(successfulCount) / totalDuration.Seconds()
	successRate := float64(successfulCount) / float64(totalRequests) * 100

	return TestResult{
		TotalRequests:     totalRequests,
		Successful:        successfulCount,
		Failed:            failedCount,
		Duration:          totalDuration,
		RequestsPerSecond: requestsPerSecond,
		AvgLatency:        avgLatency,
		P95Latency:        p95Latency,
		P99Latency:        p99Latency,
		SuccessRate:       successRate,
		Latencies:         latencies,
	}
}
