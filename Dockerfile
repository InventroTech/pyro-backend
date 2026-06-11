FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy requirements to container and install
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app code ./src code to container's /app
COPY ./src /app

ENV DJANGO_SETTINGS_MODULE=config.settings

# Placeholders only for image build; Render injects real secrets at runtime.
RUN DJANGO_SECRET_KEY=collectstatic-build-placeholder \
    SUPABASE_JWT_SECRET=collectstatic-build-placeholder \
    DB_NAME=build DB_USER=build DB_PASSWORD=build DB_HOST=localhost DB_PORT=5432 \
    python manage.py collectstatic --noinput

# gunicorn_config post_fork starts general + optional Mixpanel background job threads
CMD ["gunicorn", "config.wsgi:application", "-c", "config/gunicorn_config.py", "--bind", "0.0.0.0:8000"]
