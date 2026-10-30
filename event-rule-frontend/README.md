# Event Rule Frontend

React-based UI for the Event Rule application. It lets you create, deploy and manage event rule
applications (Flink agent jobs), inject test events, and watch the Kafka input/output topics of a
running application in real time.

## Prerequisites

- Node.js 20.x or higher
- npm 10.x or higher

## Project Structure

```
event-rule-frontend/
├── public/                      # Static files
├── src/
│   ├── index.jsx                # Application entry point
│   ├── App.jsx                  # Root component: screen routing, agent management panel, live dashboard
│   ├── config.js                # Build-time configuration (API_URL, FLINK_DASHBOARD_URL)
│   ├── UseCaseSelector.jsx      # Landing screen: header, search/sort/view toggle, application list
│   ├── AgentScenariosPanel.jsx  # Application cards/rows: deploy, edit, delete, open dashboard
│   ├── AgentWizard.jsx          # Multi-step wizard to create/edit an application and deploy it
│   ├── AgentDiagramModal.jsx    # Architecture diagram of an application (subscriptions, entity, outputs)
│   ├── SendEventsPanel.jsx      # Run/edit/create test scenarios; "Generate Sample Event"
│   ├── SampleEventModal.jsx     # Edit and send a custom sample event to an input topic
│   ├── LiveExecutionTimeline.jsx# Timeline of recent events across the topics
│   ├── ScenarioTimeline.jsx     # Timeline preview of a scenario's events
│   ├── eventCardRegistry.jsx    # Picks the card used to render an event
│   ├── DecisionResultCard.jsx   # Card for rule-engine output (decision, score, rules, alerts, actions)
│   ├── EntityEventCard.jsx      # Card for entity/domain events
│   ├── ProcessingErrorCard.jsx  # Card for pipeline processing errors (agent-error topics)
│   ├── JsonCardRenderer.jsx     # Generic recursive JSON → card renderer (fallback)
│   ├── EventCardErrorBoundary.jsx # Isolates card rendering failures
│   ├── NotificationContainer.jsx / NotificationToast.jsx # Toast notifications for new events
│   ├── useEventStream.js        # Live events: WebSocket with HTTP polling fallback
│   ├── useKafkaNotifications.js # Detects new events and produces notifications
│   ├── *.css                    # Styles
│   └── *.test.js(x)             # Vitest test suites
├── bff/                         # Backend-for-Frontend service (Node.js) — see bff/README.md
├── Dockerfile                   # Serves the pre-built app with nginx
├── nginx.conf.template          # nginx configuration (CSP filled in at container start)
├── vite.config.js               # Vite + Vitest configuration
├── package.json                 # Node.js dependencies and scripts
└── README.md                    # This file
```

## Screens

### Event Rules Applications (landing screen)

- Lists every event rule application discovered by the BFF (`GET /api/agents`), as cards or as a list
  (view toggle), with **search** and **sort** (recently updated, name A–Z, name Z–A).
- **New Event Rules Application** opens the wizard.
- Each application shows its agents and its Flink job state, with these actions:
  - **Deploy** — uploads/submits the Flink job (`POST /api/agents/:id/start`) and polls until it is running.
  - **Open dashboard** — available only while the job is running.
  - **Edit** / **Delete** — available only while the job is not running; applications with several agents
    offer per-agent edit/delete and **+ Add Agent**.

### Wizard

Steps: **Job & Identity → Ruleset → Subscriptions → Outputs → State & Review → Publish & Deploy**.
The wizard is also used to edit an application, edit a single agent, or add an agent to an existing job.

### Application dashboard

- **Agent Management** (left pane): **Edit Agent**, **Diagram View**, **Delete Agent**, **Undeploy Agent**.
- **Send Events** (left pane): select a scenario, preview/edit its payload and **Run Scenario**;
  **Save Scenario**, **+ New Scenario**, delete and reset are supported; **Generate Sample Event** opens an
  editor pre-filled from the agent's schema and sends the event to a chosen input topic.
- **Topic panels** (right pane, resizable with the splitter): one section per input/output topic, tagged
  *Input*, *Output* or *Input & Output*, showing a received counter and the latest events (newest first)
  as cards. Output topics can declare a filter that is toggled with **Show …**.
  **Show system topics** reveals changelog and error topics.
- **Live Execution Timeline** above the topic panels, and **Clean topics** to purge the application's
  Kafka topics and clear the panels.
- Toast notifications appear when new events arrive (up to 5 visible, dismissed after 5 seconds).

### Header

Available on both screens, on the right:

- **Open Flink Dashboard** — opens the Flink web UI in a new browser tab (see `VITE_FLINK_DASHBOARD_URL`).
- **Back to applications** (dashboard only) — returns to the landing screen. Clicking the
  "Decision Intelligence - Event Rules" title does the same.
- Settings, connections and theme buttons are placeholders (not implemented yet).

## Key Components

| Component                | Description                                                                                                     |
|--------------------------|-----------------------------------------------------------------------------------------------------------------|
| `App`                    | Screen routing, BFF config, descriptor loading, topic panels, notifications                                     |
| `UseCaseSelector`        | Landing screen shell; renders `AgentScenariosPanel`                                                             |
| `AgentScenariosPanel`    | Lists applications from `GET /api/agents`; polls `GET /api/flink/status`; deploy/edit/delete actions            |
| `AgentWizard`            | Create/edit/append flows via `POST /api/agents`, `PATCH /api/agents/:id/agents/:agentId`; start/stop/jar upload |
| `SendEventsPanel`        | Loads scenarios from `GET /api/use-cases/:id/scenarios` and publishes via `POST /api/use-cases/:id/events`      |
| `eventCardRegistry`      | Chooses `ProcessingErrorCard`, `DecisionResultCard`, `EntityEventCard` or `JsonCardRenderer` for each event      |
| `useEventStream`         | WebSocket subscription (`/ws`) with HTTP polling fallback (`GET /api/use-cases/:id/events`)                     |

## Development

### Install Dependencies

```bash
npm install
```

### Run Development Server

```bash
npm start
```

The application will start on [http://localhost:3010](http://localhost:3010) (Vite opens the browser
automatically). The BFF must be running; by default the UI expects it on port 8080 of the same host.

### Build for Production

```bash
npm run build
```

The production build will be created in the `build/` directory.

## Docker

The image only serves the pre-built assets with nginx, so build the app first.

### Build Docker Image

```bash
npm run build
docker build -t event-rule-frontend .
```

### Run Docker Container

```bash
docker run -p 3010:80 event-rule-frontend
```

The application will be available at [http://localhost:3010](http://localhost:3010) (the same port as
`docker-compose.yml`).

nginx sends a `Content-Security-Policy` whose `connect-src` is filled in at container start from two
environment variables. Set them when the BFF is not on `localhost:8080`:

| Variable        | Description              | Default                 |
|-----------------|--------------------------|-------------------------|
| `BFF_URL_HTTP`  | BFF REST base URL        | `http://localhost:8080` |
| `BFF_URL_WS`    | BFF WebSocket base URL   | `ws://localhost:8080`   |

## Environment Variables

These are Vite **build-time** variables: they are read when running `npm start` or `npm run build` and
are baked into the bundle, so changing them requires a rebuild.

| Variable                    | Description                                                                              | Default                          |
|-----------------------------|------------------------------------------------------------------------------------------|----------------------------------|
| `VITE_API_URL`              | BFF REST base URL. The WebSocket URL is derived from it (`http→ws`, `https→wss`, `/ws`). | `<protocol>//<page-host>:8080`   |
| `VITE_FLINK_DASHBOARD_URL`  | Flink web dashboard opened in a new tab by the header's "Open Flink Dashboard" button.   | `<protocol>//<page-host>:8081`   |

To set environment variables during development, create a `.env.local` file:

```
VITE_API_URL=http://localhost:8080
VITE_FLINK_DASHBOARD_URL=http://localhost:8081
```

## Available Scripts

- `npm start` — Runs the app in development mode (Vite)
- `npm run build` — Builds the app for production
- `npm test` — Runs the test suite (Vitest)
- `npm run test:watch` — Runs the tests in watch mode
- `npm run lint` — Lints `src` with ESLint
- `npm run preview` — Previews the production build locally

## Features

- **Application management**: Create, edit, deploy, undeploy and delete event rule applications from the UI
- **Multi-agent applications**: Several agents can share one Flink job; each can be edited or deleted individually
- **Guided wizard**: Six-step flow from identity and ruleset to publishing and deploying the Flink job
- **Architecture diagram**: Per-application view of subscriptions, entity state and outputs
- **Send Events**: Run, edit, save and create test scenarios, or send a custom sample event
- **Adaptive event cards**: Decision results, entity events and processing errors rendered by shape, with a generic JSON fallback
- **Input/Output topic panels**: Every topic of the application with its role, received counter and latest events
- **Real-time Event Streaming**: Events pushed via WebSocket
- **Automatic Reconnection**: Reconnects after 5 seconds if the connection is lost; falls back to HTTP polling (every 2 seconds)
  after 3 failed initial connection attempts or 20 reconnects
- **Event History**: Shows the most recent events per topic, newest first; the limit comes from the BFF (`GET /api/config`, 50 by default)
- **Notifications**: Toasts for newly arrived events
- **Flink Dashboard shortcut**: Header button that opens the Flink web UI in a new tab

## Technology Stack

- React 18.x
- Vite 6.x
- Vitest + Testing Library
- Nginx (for production deployment)
