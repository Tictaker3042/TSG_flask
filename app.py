from flask import Flask, jsonify
import psycopg2
from psycopg2 import extras
import logging
from datetime import date

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection():
    """Создает новое соединение с базой данных"""
    try:
        conn = psycopg2.connect(
            database="tsg",
            user="administrator",
            password="root",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Connection failed: {e}")
        raise


@app.route('/api/latest_payments', methods=['GET'])
def get_latest_payments():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)

        query = """
            WITH latest_payments AS (
                SELECT 
                    a.Room_number,
                    CONCAT(fo.Owner_name, ' ', fo.Owner_surname) AS owner_full_name,
                    p.Payment_date,
                    p.Amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.Room_number 
                        ORDER BY p.Payment_date DESC NULLS LAST
                    ) AS payment_rank
                FROM Apartments a
                LEFT JOIN Flat_owner fo ON a.Room_number = fo.Room_number
                LEFT JOIN Payments p ON fo.Owner_id = p.Owner_id
            )
            SELECT 
                Room_number,
                COALESCE(owner_full_name, 'Не указан') AS owner,
                Payment_date,
                Amount
            FROM latest_payments
            WHERE payment_rank = 1
            ORDER BY Room_number;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        payments = []
        for row in results:
            # Обращаемся к полям в нижнем регистре
            payment_date = row['payment_date'].isoformat() if row['payment_date'] else "Нет платежей"

            # Аналогично проверяем остальные поля
            amount_rub = row['amount'] / 100 if row['amount'] is not None else 0.00

            payment_data = {
                "room_number": row['room_number'],  # также проверьте регистр
                "owner": row['owner'],
                "last_payment_date": payment_date,
                "amount": f"{amount_rub:.2f} руб."
            }
            payments.append(payment_data)

        return jsonify({
            "status": "success",
            "results": len(payments),
            "data": payments
        }), 200

    except psycopg2.DatabaseError as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Ошибка базы данных"
        }), 500

    except Exception as e:
        logger.error(f"System error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Внутренняя ошибка сервера"
        }), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
