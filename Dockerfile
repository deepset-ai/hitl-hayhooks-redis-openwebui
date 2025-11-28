FROM deepset/hayhooks:main

# Install git for pip to clone from GitHub
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["hayhooks", "run", "--host", "0.0.0.0"]
