FROM python:3.10-alpine

# Atualiza e instala ferramentas de build, apenas para garantir compatibilidade em Alpine
RUN apk update && apk add --no-cache gcc musl-dev

WORKDIR /app

# Copia os arquivos
COPY requirements.txt .
COPY load_generator.py .

# Instala PyMySQL
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "load_generator.py"]