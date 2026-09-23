# Event Rule Frontend

React-based real-time dashboard for the Event Rule application.

## Prerequisites

- Node.js 20.x or higher
- npm 10.x or higher

## Project Structure

```
event-rule-frontend/
├── public/              # Static files
├── src/
│   ├── App.js           # Main application: dashboard, FraudResultCard, RiskBar, DecisionBadge
│   ├── App.css          # Application styles
│   ├── AccountCard.js   # Structured card for account-created events
│   ├── SendEventsPanel.js  # One-click demo scenario injection panel
│   ├── UseCaseSelector.js  # Landing screen for picking a use case
│   ├── UseCaseSelector.css
│   ├── index.jsx        # Application entry point
│   └── index.css        # Global styles
├── bff/                 # Backend-for-Frontend service (Node.js)
├── Dockerfile           # Docker build configuration
├── nginx.conf           # Nginx configuration for production
├── package.json         # Node.js dependencies and scripts
└── README.md            # This file
```

## Key Components

| Component         | Description                                                                                                     |
|-------------------|-----------------------------------------------------------------------------------------------------------------|
| `UseCaseSelector` | Landing screen; fetches available use cases from `GET /api/use-cases`                                           |
| `AccountCard`     | Renders account-created events with status badge, balance, and country                                          |
| `FraudResultCard` | Renders rule-engine output: `DecisionBadge`, `RiskBar`, triggered rules, alerts, and recommended actions        |
| `SendEventsPanel` | Loads scenarios from `GET /api/use-cases/:id/scenarios` and publishes them via `POST /api/use-cases/:id/events` |
| `App` (main)      | WebSocket lifecycle, per-topic event state, adaptive card rendering                                             |

## Development

### Install Dependencies

```bash
npm install
```

### Run Development Server

```bash
npm start
```

The application will start on [http://localhost:3010](http://localhost:3010).

### Build for Production

```bash
npm run build
```

The production build will be created in the `build/` directory.

## Docker

### Build Docker Image

```bash
docker build -t event-rule-frontend .
```

### Run Docker Container

```bash
docker run -p 80:80 event-rule-frontend
```

The application will be available at [http://localhost](http://localhost).

## Environment Variables

| Variable        | Description                                   | Default                 |
|-----------------|-----------------------------------------------|-------------------------|
| `VITE_WS_URL`   | BFF WebSocket URL                             | `ws://localhost:8080`   |
| `VITE_API_URL`  | BFF REST base URL (used by `SendEventsPanel`) | `http://localhost:8080` |

To set environment variables during development, create a `.env.local` file:

```
VITE_WS_URL=ws://localhost:8080
VITE_API_URL=http://localhost:8080
```

## Available Scripts

- `npm start` — Runs the app in development mode (Vite)
- `npm run build` — Builds the app for production
- `npm test` — Runs the test suite (Vitest)
- `npm run preview` — Previews the production build locally

## Features

- **Use-Case Selection**: Landing screen; available use cases are auto-discovered by the BFF
- **Account Card**: Structured display for account-created events with status badge (ACTIVE / FROZEN / SUSPENDED / …)
- **Fraud Result Card**: Rich display of `RuleEngineResult` — decision badge, colour-coded risk score bar, triggered rules list with severity colours, alerts with type and message, and recommended actions
- **Send Events Panel**: One-click injection of pre-built demo scenarios (banking use case); scenarios loaded dynamically from the BFF
- **Outbound Topic Panel**: Dedicated section for rule-engine output events (`fraud-detection-results`)
- **Real-time Event Streaming**: Events pushed via WebSocket with no polling
- **Automatic Reconnection**: Reconnects after 3 seconds if the connection is lost
- **Event History**: Shows the last 10 events per topic, newest first

## Technology Stack

- React 18.x
- Vite 6.x
- Nginx (for production deployment)

