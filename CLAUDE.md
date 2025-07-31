# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Application Overview

This is a **Mabinogi Mobile ranking data crawler and API service** that collects character ranking data from the official game website and provides RESTful API endpoints for querying rankings, population statistics, and class distributions.

## Running the Application

### Start the API Server
```bash
python server.py
```
The FastAPI server will start with automatic background tasks for data collection.

### Run Standalone Crawlers
```bash
python sequential_ranking_crawler.py    # Multi-threaded ranking data crawler
python balanced_ranking_crawler.py      # Balanced load crawler
python simple_crawler.py               # Basic crawler for testing
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Development Commands

### Testing
The project uses custom test scripts (no formal testing framework):
```bash
python test/smoke_test.py          # Playwright browser testing
python test/crawler.py             # Selenium crawler testing  
python test.py                     # Complete DOM manipulation test
python test_complete_queue.py      # Queue system testing
```

### Database Operations
```bash
# Database schema is in sql/ directory
# Connection managed via service/db_session.py with KST timezone
```

## Core Architecture

### Main Components
- **`server.py`** - FastAPI web server with API endpoints and IP whitelist middleware
- **`sequential_ranking_crawler.py`** - Multi-threaded ranking data crawler with connection pooling
- **`api/rankData.py`** - Primary API logic for character data retrieval
- **`service/`** directory - Core business logic modules

### Service Layer
- **`background_tasks.py`** - Manages automated data collection workers
- **`db.py`** & **`db_session.py`** - PostgreSQL database operations and connection management  
- **`async_db.py`** - Asynchronous database operations
- **`full_data.py`** - Selenium-based web scraping with Chrome WebDriver pooling
- **`driver_pool.py`** - Chrome WebDriver connection pooling (5 concurrent drivers)
- **`search_queue.py`** & **`search_worker.py`** - PostgreSQL-based search request queue system
- **`fast_ranking_service.py`** - High-speed ranking service with persistent caching
- **`persistent_ranking_cache.py`** - Persistent page caching (3-5x performance improvement)
- **`population.py`** - Server population data collection
- **`population_statistics.py`** - Population analytics and chart generation using matplotlib
- **`html_image_converter.py`** - HTML to image conversion service

### Database Schema
- **`mabinogi_ranking`** - Character ranking data (rank, power, server, class, retrieved_at)
- **`mabinogi_population_statistics`** - Historical population data by date/server/class
- All timestamps use Korean Standard Time (KST)

### Key API Endpoints
- **`POST /search`** - Character ranking search (queued processing)
- **`POST /search/sync`** - Character ranking search (immediate processing)
- **`GET /search/status/{job_id}`** - Check search job status and results
- **`GET /population`** - Current server population data  
- **`GET /class-chart`** - Job class distribution charts
- **`POST /html_to_image`** - Convert HTML tables to images
- **`GET /cache-status`** - Check ranking cache status
- **`GET /search/queue/status`** - Check search queue and worker status

### Background Processing
The application runs automated tasks:
- Character data updates every 30 seconds
- Population statistics collection hourly
- Class distribution analysis hourly  
- Daily statistics archival at midnight KST

### Technology Stack
- **FastAPI + Uvicorn** for the web API
- **Selenium + BeautifulSoup** for web scraping with Chrome WebDriver pooling
- **PostgreSQL + SQLAlchemy** for data persistence
- **Matplotlib + Chart.js** for data visualization
- **Korean timezone (KST)** handling throughout the application

### Performance Architecture
The application uses multi-layered optimization:
- **Persistent ranking cache** - Keeps 3 active browser pages for 3-5x performance improvement
- **WebDriver connection pooling** - 5 concurrent Chrome drivers for parallel scraping
- **PostgreSQL-based queue system** - Background processing with JSON serialization
- **Database caching** - 10-minute cache threshold for frequent requests

### Development Notes
- The application includes IP whitelist middleware for production security
- All data collection respects Korean timezone (KST) for accurate timestamp handling
- Connection pooling is implemented for both database and WebDriver connections
- Error handling and structured logging are implemented throughout
- Testing files are in `test/` directory using Playwright and Selenium
- No formal linting/formatting tools configured - manual code quality management