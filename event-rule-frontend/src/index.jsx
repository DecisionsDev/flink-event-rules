/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';

const root = ReactDOM.createRoot(document.getElementById('root'));

function StartupError({ error }) {
  const message = (error && error.message) || String(error);
  return (
    <div role="alert" style={{ fontFamily: 'sans-serif', margin: '2rem auto', maxWidth: 720, padding: '1rem', border: '1px solid #da1e28', background: '#fff1f1', color: '#161616' }}>
      <h1 style={{ fontSize: '1.25rem', marginTop: 0 }}>The application failed to start</h1>
      <p><code>{message}</code></p>
      <p>
        Open the browser developer console for details. If the page is served over plain HTTP from a
        remote host, some browser APIs are unavailable; use HTTPS or contact your administrator.
      </p>
    </div>
  );
}

// Catches errors thrown while rendering the component tree so a render failure
// shows a message instead of unmounting the whole UI.
class RootErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    console.error('[RootErrorBoundary] Render failed:', error, info.componentStack);
  }

  render() {
    return this.state.error ? <StartupError error={this.state.error} /> : this.props.children;
  }
}

// App is imported dynamically: a static import would evaluate App (and its
// module-level code) before this file runs, so an exception there could never
// be reported and the page would stay blank.
import('./App')
  .then(({ default: App }) => {
    root.render(
      <React.StrictMode>
        <RootErrorBoundary>
          <App />
        </RootErrorBoundary>
      </React.StrictMode>
    );
  })
  .catch((error) => {
    console.error('[startup] Failed to load the application:', error);
    root.render(<StartupError error={error} />);
  });

