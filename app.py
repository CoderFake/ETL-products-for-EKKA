import os
import json
import re
import asyncio
from datetime import datetime
import numpy as np
import aiomysql
from flask import Flask, request, jsonify
import logging
from decouple import config
from base64 import urlsafe_b64decode, urlsafe_b64encode
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

app = Flask(__name__)


logging.basicConfig(filename='error.log', level=logging.ERROR)


DB_HOST = config('DB_HOST')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_DATABASE = config('DB_DATABASE')
AES_KEY = urlsafe_b64decode(config('AES_KEY'))
LOCAL_DIR = config('LOCAL_DIR')


def decrypt(encrypted_data):
    encrypted_data = urlsafe_b64decode(encrypted_data)
    iv = encrypted_data[:16]
    encrypted_message = encrypted_data[16:]

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    decrypted_padded_message = decryptor.update(encrypted_message) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    decrypted_message = unpadder.update(decrypted_padded_message) + unpadder.finalize()

    return decrypted_message.decode('utf-8')


def encrypt(data):
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(data.encode()) + padder.finalize()

    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    return urlsafe_b64encode(iv + encrypted_data).decode('utf-8')


async def timestamp_to_datetime(timestamp):
    """Chuyển đổi Unix timestamp sang datetime định dạng 'YYYY-MM-DD HH:mm:ss'."""
    return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')


async def concatenate_and_custom_slugify(product_name, product_id):
    """Tạo một slug từ tên sản phẩm và ID, loại bỏ tất cả các ký tự đặc biệt."""
    cleaned_name = re.sub(r'[^\w\s]', '', product_name)
    slugified_name = cleaned_name.replace(' ', '-').lower()
    return f"{slugified_name}-{product_id}"


async def get_image_url(images, image_type):
    """Hàm trợ giúp để lấy URL hình ảnh hoặc trả về chuỗi trống nếu không có sẵn."""
    return images.get(image_type, {}).get('imageURL', '')


async def transform_product_data(product_data):
    colors = [product_data.get("baseColour", "")]
    if product_data["colour1"] != "NA" and product_data["colour1"].strip():
        colors.append(product_data["colour1"])
    if product_data["colour2"] != "NA" and product_data["colour2"].strip():
        colors.append(product_data["colour2"])

    variations = [
        {
            "variation_category": "Color",
            "variation_value": option,
            "is_active": 1
        } for option in colors
    ]

    variations.extend([
        {
            "variation_category": option.get("name", ""),
            "variation_value": option.get("value", ""),
            "is_active": 1 if option.get("active") else 0
        } for option in product_data.get("styleOptions", [])
    ])

    transformed_data = {
        "product_name": product_data.get("productDisplayName", ""),
        "slug": await concatenate_and_custom_slugify(product_data.get("productDisplayName", ""),
                                                     str(product_data.get("id", ""))),
        "category_main": product_data.get("masterCategory", {}).get("typeName", "").lower().replace(' ', '-'),
        "sub_category": product_data.get("subCategory", {}).get("typeName", "").lower().replace(' ', '-'),
        "mrp_price": product_data.get("price", ""),
        "price": product_data.get("discountedPrice", ""),
        "rating": product_data.get("myntraRating", ""),
        "short_desp": product_data.get("articleType", {}).get("typeName", ""),
        "description": product_data.get("productDescriptors", {}).get("description", {}).get("value", ""),
        "stock": np.random.randint(1, 200),
        "is_available": product_data.get("codEnabled", False),
        "created_date": await timestamp_to_datetime(product_data.get("catalogAddDate", "")),
        "modified_date": await timestamp_to_datetime(product_data.get("catalogAddDate", "")),
        "brand_name": product_data.get("brandName", ""),
        "gender": product_data.get("gender", ""),
        "season": product_data.get("season", ""),
        "year": product_data.get("year", ""),
        "variations": variations,
        "images": {
            "default": await get_image_url(product_data.get("styleImages", {}), "default"),
            "back": await get_image_url(product_data.get("styleImages", {}), "back"),
            "front": await get_image_url(product_data.get("styleImages", {}), "front"),
            "top": await get_image_url(product_data.get("styleImages", {}), "top"),
            "left": await get_image_url(product_data.get("styleImages", {}), "left"),
            "right": await get_image_url(product_data.get("styleImages", {}), "right"),
            "bottom": await get_image_url(product_data.get("styleImages", {}), "bottom")
        }
    }
    return transformed_data


async def insert_product_images(cursor, product_id, images):
    for img_type, img_url in images.items():
        img_url_to_insert = img_url if img_url else None
        await cursor.execute("""
            INSERT INTO store_productimage (product_id, image_type, image_url)
            VALUES (%s, %s, %s)
        """, (product_id, img_type, img_url_to_insert))


async def insert_variations(cursor, product_id, variations):
    for var in variations:
        await cursor.execute("""
            INSERT INTO store_variation (Product_id, Variation_category, Variation_value, is_active)
            VALUES (%s, %s, %s, %s)
        """, (product_id, var['variation_category'], var['variation_value'], var['is_active']))


async def get_sub_category_details(cursor, sub_category_name):
    await cursor.execute(
        "SELECT id, category_id FROM category_subcategory WHERE sub_category_name = %s",
        (sub_category_name,)
    )
    result = await cursor.fetchone()
    return result if result else (None, None)


async def process_file(file_path, pool):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                with open(file_path, 'r') as file:
                    product_data = json.load(file)
                transformed_product_data = await transform_product_data(product_data)

                sub_category_id, category_main_id = await get_sub_category_details(cursor, transformed_product_data[
                    'sub_category'])

                if not sub_category_id or not category_main_id:
                    raise ValueError(f"Thiếu ID danh mục cho sản phẩm trong tệp {file_path}")

                await cursor.execute("""
                    INSERT INTO store_product (
                        product_name, slug, category_main_id, sub_category_id, mrp_price, price, rating,
                        short_desp, description, stock, is_available, brand_name, gender, season, year,
                        created_date, modified_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                    )
                """, (
                    transformed_product_data["product_name"].lower(), transformed_product_data["slug"],
                    category_main_id,
                    sub_category_id, transformed_product_data["mrp_price"] / 100,
                    transformed_product_data["price"] / 100, transformed_product_data["rating"],
                    transformed_product_data["short_desp"], transformed_product_data["description"],
                    transformed_product_data["stock"],
                    transformed_product_data["is_available"], transformed_product_data["brand_name"],
                    transformed_product_data["gender"],
                    transformed_product_data["season"], transformed_product_data["year"]
                ))

                product_id = cursor.lastrowid
                await insert_variations(cursor, product_id, transformed_product_data['variations'])
                await insert_product_images(cursor, product_id, transformed_product_data['images'])

                await connection.commit()
                print(f"Đã chèn sản phẩm từ {file_path}")

    except Exception as e:
        logging.error(f"Không thể xử lý tệp {file_path}. Lỗi: {e}")


@app.route('/get-folders', methods=['GET'])
def get_folders():
    """Trả về danh sách các thư mục trong LOCAL_DIR"""
    try:
        folders = [name for name in os.listdir(LOCAL_DIR) if os.path.isdir(os.path.join(LOCAL_DIR, name))]
        encrypted_folders = [encrypt(folder) for folder in folders]
        return jsonify({"folders": encrypted_folders})
    except Exception as e:
        logging.error(f"Không thể lấy danh sách thư mục từ LOCAL_DIR. Lỗi: {e}")
        return jsonify({"status": "error", "message": "Unable to list folders in LOCAL_DIR"}), 500


@app.route('/process-folder', methods=['POST'])
async def process_folder():
    data = request.get_json()
    encrypted_data = data.get('data')

    try:
        decrypted_data = json.loads(decrypt(encrypted_data))
        folder_name = decrypted_data.get('folder_name')
    except Exception as e:
        return jsonify({"status": "error", "message": "Invalid encryption or decryption process."})

    folder_path = os.path.join(LOCAL_DIR, folder_name)
    if not os.path.isdir(folder_path):
        return jsonify({"status": "error", "message": f"Folder {folder_name} not found."})

    pool = await aiomysql.create_pool(host=DB_HOST, db=DB_DATABASE, user=DB_USER, password=DB_PASSWORD, minsize=1,
                                      maxsize=10)
    try:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.json')]
        tasks = [process_file(file, pool) for file in files]
        await asyncio.gather(*tasks)
    finally:
        pool.close()
        await pool.wait_closed()
    return jsonify({"status": "success", "message": f"Folder {folder_name} processed successfully."})


if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)
