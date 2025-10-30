# People Curation Actions API

This project provides a FastAPI server with actions for cleaning and validating people data for an OpenAI agent.

## Setup

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set environment variables:**
    Create a `.env` file in the root directory and add your API key:
    ```
    ACTIONS_API_KEY=your_secret_api_key
    ```

3.  **Run the server:**
    ```bash
    uvicorn server:app --reload
    ```

## Deployment on Render

This project is configured for deployment on Render using the `render.yaml` blueprint.

1.  Create a new "Blueprint" service on Render.
2.  Connect your GitHub repository.
3.  Render will automatically build and deploy the service.
4.  You will need to add your `ACTIONS_API_KEY` as a secret environment variable in the Render dashboard.
