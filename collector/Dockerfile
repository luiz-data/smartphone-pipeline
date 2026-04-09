FROM python:3.11-slim

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia e instala dependências primeiro (aproveita cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Ponto de entrada
CMD ["python", "main.py"]