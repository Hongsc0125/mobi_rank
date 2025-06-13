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

### Run Standalone Crawler
```bash
python sequential_ranking_crawler.py
```
Runs the ranking data crawler independently for testing or manual data collection.

### Install Dependencies
```bash
pip install -r requirements.txt
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
- **`full_data.py`** - Selenium-based web scraping with Chrome WebDriver pooling
- **`population.py`** - Server population data collection
- **`population_statistics.py`** - Population analytics and chart generation using matplotlib
- **`driver_pool.py`** - Chrome WebDriver connection pooling for concurrent scraping

### Database Schema
- **`mabinogi_ranking`** - Character ranking data (rank, power, server, class, retrieved_at)
- **`mabinogi_population_statistics`** - Historical population data by date/server/class
- All timestamps use Korean Standard Time (KST)

### Key API Endpoints
- **`POST /search`** - Search character rankings by server and name
- **`GET /population`** - Current server population data  
- **`GET /class-chart`** - Job class distribution charts
- **`POST /html_to_image`** - Convert HTML tables to images

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

### Development Notes
- The application includes IP whitelist middleware for production security
- All data collection respects Korean timezone (KST) for accurate timestamp handling
- Connection pooling is implemented for both database and WebDriver connections
- Error handling and structured logging are implemented throughout
- Testing files are in `test/` directory using Playwright and Selenium