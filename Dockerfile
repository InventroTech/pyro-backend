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

# Collect static files
RUN python manage.py collectstatic --noinput

# Set environment variables
ENV DJANGO_SETTINGS_MODULE=config.settings

# gunicorn_config post_fork starts general + optional Mixpanel background job threads
CMD ["gunicorn", "config.wsgi:application", "-c", "config/gunicorn_config.py", "--bind", "0.0.0.0:8000"]
