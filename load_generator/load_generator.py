import pymysql
import time
import random

# Configurações do banco de dados
DB_HOST = "mysql"
DB_NAME = "desafio_db"
DB_USER = "user"
DB_PASSWORD = "password"

def get_db_connection():
    """Tenta estabelecer a conexão com o MySQL."""
    conn = None
    while not conn:
        try:
            conn = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=3306 # Porta padrão do MySQL
            )
            print("Conexão com o banco de dados MySQL estabelecida com sucesso.")
            return conn
        except Exception as e:
            print(f"Falha na conexão MySQL: {e}. Tentando novamente em 5s...")
            time.sleep(5)

def generate_load(conn):
    """Executa uma operação CRUD aleatória."""
    operation = random.choice(['INSERT', 'SELECT', 'UPDATE', 'DELETE'])
    
    try:
        with conn.cursor() as cur:
            
            if operation == 'INSERT':
                amount = round(random.uniform(10.0, 1000.0), 2)
                cur.execute("INSERT INTO transactions (operation_type, amount) VALUES (%s, %s)", (operation, amount))
                print(f"Operação: {operation} - Adicionado {amount}")
                
            elif operation == 'SELECT':
                cur.execute("SELECT COUNT(*) FROM transactions")
                count = cur.fetchone()[0]
                print(f"Operação: {operation} - Total de registros: {count}")
                
            elif operation == 'UPDATE':
                cur.execute("SELECT id FROM transactions ORDER BY RAND() LIMIT 1")
                result = cur.fetchone()
                if result:
                    id_to_update = result[0]
                    new_amount = round(random.uniform(10.0, 1000.0), 2)
                    cur.execute("UPDATE transactions SET amount = %s, operation_type = %s WHERE id = %s", (new_amount, operation, id_to_update))
                    print(f"Operação: {operation} - Atualizado ID {id_to_update} para {new_amount}")
                else:
                    print(f"Operação: {operation} - Nenhuma linha para atualizar.")
                    
            elif operation == 'DELETE':
                cur.execute("SELECT COUNT(*) FROM transactions")
                count = cur.fetchone()[0]
                if count > 5:
                    cur.execute("DELETE FROM transactions WHERE id IN (SELECT id FROM transactions ORDER BY RAND() LIMIT 1)")
                    print(f"Operação: {operation} - Deletado 1 registro")
                else:
                    print(f"Operação: {operation} - Manter mais de 5 registros. Não deletado.")

        conn.commit()

    except Exception as e:
        print(f"Erro durante a operação {operation}: {e}")
        conn.rollback()


if __name__ == "__main__":
    conn = get_db_connection()
    while True:
        generate_load(conn)
        # Intervalo aleatório entre 0.5 e 2.0 segundos
        time.sleep(random.uniform(0.5, 2.0))