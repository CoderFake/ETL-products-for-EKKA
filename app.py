import os
import json
import asyncio
import logging
import aiomysql
from flask import Flask, request, jsonify
from decouple import config
from base64 import urlsafe_b64decode, urlsafe_b64encode
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import numpy as np
from datetime import datetime

app = Flask(__name__)

logging.basicConfig(filename='flask_api.log', level=logging.INFO)

AES_KEY = urlsafe_b64decode(config('AES_KEY'))
LOCAL_DIR = config('LOCAL_DIR')
DB_HOST = config('DB_HOST')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASS')
DB_DATABASE = config('DB_NAME')


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
    if timestamp:
        try:
            return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


async def concatenate_and_custom_slugify(product_name, product_id):
    cleaned_name = ''.join(char for char in product_name if char.isalnum() or char.isspace())
    slugified_name = cleaned_name.replace(' ', '-').lower()
    return f"{slugified_name}-{product_id}"


async def collect_categories(directory):
    categories = {}
    subcategories = {}

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                try:
                    with open(os.path.join(root, file), 'r') as f:
                        json_data = json.load(f)

                    product_data = json_data.get('data', json_data)

                    if isinstance(product_data, dict):
                        master_category = product_data.get('masterCategory', {})
                        sub_category = product_data.get('subCategory', {})

                        if isinstance(master_category,
                                      dict) and 'typeName' in master_category and 'id' in master_category:
                            cat_name = master_category['typeName'].strip()
                            cat_id = master_category['id']
                            if cat_name and cat_id:
                                categories[cat_name] = {
                                    'id': cat_id,
                                    'name': cat_name,
                                    'slug': cat_name.lower().replace(' ', '-')
                                }

                        if isinstance(sub_category, dict) and 'typeName' in sub_category and 'id' in sub_category:
                            subcat_name = sub_category['typeName'].strip()
                            subcat_id = sub_category['id']
                            if subcat_name and subcat_id:
                                parent_cat = master_category.get('typeName', '').strip()
                                subcategories[subcat_name] = {
                                    'id': subcat_id,
                                    'name': subcat_name,
                                    'slug': subcat_name.lower().replace(' ', '-'),
                                    'parent_category': parent_cat
                                }
                except Exception as e:
                    logging.error(f"Error processing {file}: {str(e)}")

    return categories, subcategories


async def import_categories_to_db(categories, subcategories, pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            for category in categories.values():
                try:
                    await cursor.execute(
                        "SELECT id FROM category_categorymain WHERE category_name = %s",
                        (category['name'],)
                    )
                    result = await cursor.fetchone()

                    if not result:
                        await cursor.execute("""
                            INSERT INTO category_categorymain 
                            (category_name, slug, description, parent_category_id) 
                            VALUES (%s, %s, %s, NULL)
                        """, (
                            category['name'],
                            category['slug'],
                            f"Description for {category['name']}"
                        ))
                        logging.info(f"Inserted category: {category['name']}")
                    else:
                        logging.info(f"Category already exists: {category['name']}")

                except Exception as e:
                    logging.error(f"Error inserting category {category['name']}: {str(e)}")

            await conn.commit()

            for subcategory in subcategories.values():
                try:
                    await cursor.execute(
                        "SELECT id FROM category_subcategory WHERE sub_category_name = %s",
                        (subcategory['name'],)
                    )
                    result = await cursor.fetchone()

                    if not result:
                        await cursor.execute(
                            "SELECT id FROM category_categorymain WHERE category_name = %s",
                            (subcategory['parent_category'],)
                        )
                        parent_result = await cursor.fetchone()

                        if parent_result:
                            parent_id = parent_result[0]
                            await cursor.execute("""
                                INSERT INTO category_subcategory 
                                (sub_category_name, slug, description, category_id, is_active) 
                                VALUES (%s, %s, %s, %s, %s)
                            """, (
                                subcategory['name'],
                                subcategory['slug'],
                                f"Description for {subcategory['name']}",
                                parent_id,
                                True
                            ))
                            logging.info(
                                f"Inserted subcategory: {subcategory['name']} under {subcategory['parent_category']}")
                        else:
                            logging.warning(f"Parent category not found for subcategory: {subcategory['name']}")
                    else:
                        logging.info(f"Subcategory already exists: {subcategory['name']}")

                except Exception as e:
                    logging.error(f"Error inserting subcategory {subcategory['name']}: {str(e)}")

            await conn.commit()


async def get_category_id_by_name(cursor, name):
    if not name:
        return None

    category_name = name.strip()
    await cursor.execute(
        "SELECT id FROM category_categorymain WHERE category_name = %s",
        (category_name,)
    )
    result = await cursor.fetchone()
    return result[0] if result else None


async def get_subcategory_id_by_name(cursor, name):
    if not name:
        return None, None

    subcategory_name = name.strip()
    await cursor.execute(
        "SELECT id, category_id FROM category_subcategory WHERE sub_category_name = %s",
        (subcategory_name,)
    )
    result = await cursor.fetchone()
    return result if result else (None, None)


async def transform_product_data(cursor, product_data):
    colors = []

    base_colour = product_data.get("baseColour", "")
    if base_colour and base_colour != "NA" and isinstance(base_colour, str) and base_colour.strip():
        colors.append(base_colour)

    colour1 = product_data.get("colour1", "NA")
    if colour1 != "NA" and isinstance(colour1, str) and colour1.strip():
        colors.append(colour1)

    colour2 = product_data.get("colour2", "NA")
    if colour2 != "NA" and isinstance(colour2, str) and colour2.strip():
        colors.append(colour2)

    variations = []
    for color in colors:
        if color:
            variations.append({
                "variation_category": "Color",
                "variation_value": color,
                "is_active": 1
            })

    style_options = product_data.get("styleOptions", [])
    if isinstance(style_options, list):
        for option in style_options:
            if isinstance(option, dict):
                variation_name = option.get("name", "")
                variation_value = option.get("value", "")
                is_active = 1 if option.get("active", False) else 0

                if variation_name and variation_value:
                    variations.append({
                        "variation_category": variation_name,
                        "variation_value": variation_value,
                        "is_active": is_active
                    })

    master_category = product_data.get("masterCategory", {})
    sub_category = product_data.get("subCategory", {})

    master_category_name = ""
    sub_category_name = ""

    if isinstance(master_category, dict) and "typeName" in master_category:
        master_category_name = master_category.get("typeName", "")

    if isinstance(sub_category, dict) and "typeName" in sub_category:
        sub_category_name = sub_category.get("typeName", "")

    category_main_id = await get_category_id_by_name(cursor, master_category_name)
    sub_category_result = await get_subcategory_id_by_name(cursor, sub_category_name)

    if sub_category_result:
        sub_category_id, parent_category_id = sub_category_result
    else:
        sub_category_id, parent_category_id = None, None

    if not category_main_id:
        category_main_id = parent_category_id

    images = {}
    style_images = product_data.get("styleImages", {})
    image_types = ["default", "back", "front", "top", "left", "right", "bottom"]

    for img_type in image_types:
        image_data = style_images.get(img_type, {})
        if isinstance(image_data, dict):
            image_url = image_data.get("imageURL", "")
            if image_url:
                images[img_type] = image_url

    transformed_data = {
        "product_name": product_data.get("productDisplayName", ""),
        "slug": await concatenate_and_custom_slugify(
            product_data.get("productDisplayName", ""),
            str(product_data.get("id", ""))
        ),
        "category_main_id": category_main_id,
        "sub_category_id": sub_category_id,
        "mrp_price": float(product_data.get("price", 0)) / 100,
        "price": float(product_data.get("discountedPrice", 0)) / 100,
        "rating": product_data.get("myntraRating", 0),
        "short_desp": (product_data.get("articleType", {}).get("typeName", "") or ""),
        "description": (product_data.get("productDescriptors", {}).get("description", {}).get("value", "") or ""),
        "stock": np.random.randint(1, 200),
        "is_available": product_data.get("codEnabled", False),
        "created_date": await timestamp_to_datetime(product_data.get("catalogAddDate", "")),
        "modified_date": await timestamp_to_datetime(product_data.get("catalogAddDate", "")),
        "brand_name": product_data.get("brandName", ""),
        "gender": product_data.get("gender", ""),
        "season": product_data.get("season", ""),
        "year": product_data.get("year", ""),
        "variations": variations,
        "images": images
    }

    return transformed_data


async def process_file(file_path, pool):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                with open(file_path, 'r') as file:
                    json_data = json.load(file)

                if 'data' in json_data and isinstance(json_data['data'], dict):
                    product_data = json_data['data']
                else:
                    product_data = json_data

                if not isinstance(product_data, dict):
                    raise ValueError(f"Invalid product data in file {file_path}")

                transformed_product_data = await transform_product_data(cursor, product_data)

                if not transformed_product_data["category_main_id"] or not transformed_product_data["sub_category_id"]:
                    raise ValueError(f"Missing category ID for product in file {file_path}")

                await cursor.execute("""
                    INSERT INTO store_product (
                        product_name, slug, category_main_id, sub_category_id, mrp_price, price, rating,
                        short_desp, description, stock, is_available, brand_name, gender, season, year,
                        created_date, modified_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                    )
                """, (
                    transformed_product_data["product_name"], transformed_product_data["slug"],
                    transformed_product_data["category_main_id"], transformed_product_data["sub_category_id"],
                    transformed_product_data["mrp_price"], transformed_product_data["price"],
                    transformed_product_data["rating"], transformed_product_data["short_desp"],
                    transformed_product_data["description"], transformed_product_data["stock"],
                    transformed_product_data["is_available"], transformed_product_data["brand_name"],
                    transformed_product_data["gender"], transformed_product_data["season"],
                    transformed_product_data["year"]
                ))

                product_id = cursor.lastrowid

                for variation in transformed_product_data['variations']:
                    await cursor.execute("""
                        INSERT INTO store_variation (Product_id, Variation_category, Variation_value, is_active)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        product_id,
                        variation['variation_category'],
                        variation['variation_value'],
                        variation['is_active']
                    ))

                for img_type, img_url in transformed_product_data['images'].items():
                    if img_url:
                        await cursor.execute("""
                            INSERT INTO store_productimage (product_id, image_type, image_url)
                            VALUES (%s, %s, %s)
                        """, (product_id, img_type, img_url))

                await connection.commit()
                logging.info(f"Successfully imported product from {file_path}")

    except Exception as e:
        logging.error(f"Failed to process file {file_path}. Error: {e}")


async def process_folder(folder_path, pool):
    tasks = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            file_path = os.path.join(folder_path, file_name)
            task = asyncio.create_task(process_file(file_path, pool))
            tasks.append(task)

    await asyncio.gather(*tasks)


async def process_folder_async(folder_name):
    try:
        logging.info(f"Processing folder: {folder_name}")

        folder_path = os.path.join(LOCAL_DIR, folder_name)

        if not os.path.isdir(folder_path):
            return {"status": "error", "message": f"Folder {folder_name} not found"}

        pool = await aiomysql.create_pool(
            host=DB_HOST,
            port=3306,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_DATABASE,
            autocommit=False
        )

        categories, subcategories = await collect_categories(folder_path)
        await import_categories_to_db(categories, subcategories, pool)

        await process_folder(folder_path, pool)

        pool.close()
        await pool.wait_closed()

        return {"status": "success", "message": f"Folder {folder_name} processed successfully"}

    except Exception as e:
        logging.error(f"Error processing folder {folder_name}: {e}")
        return {"status": "error", "message": str(e)}


@app.route('/get-folders', methods=['GET'])
def get_folders():
    try:
        folders = [name for name in os.listdir(LOCAL_DIR) if os.path.isdir(os.path.join(LOCAL_DIR, name))]
        encrypted_folders = [encrypt(folder) for folder in folders]
        return jsonify({"folders": encrypted_folders})
    except Exception as e:
        logging.error(f"Error getting folders: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/process-folder', methods=['POST'])
def process_folder_endpoint():
    try:
        data = request.get_json()
        encrypted_data = data.get('data')

        if not encrypted_data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        decrypted_data = json.loads(decrypt(encrypted_data))
        folder_name = decrypted_data.get('folder_name')

        if not folder_name:
            return jsonify({"status": "error", "message": "No folder name provided"}), 400

        result = asyncio.run(process_folder_async(folder_name))
        return jsonify(result)

    except Exception as e:
        logging.error(f"Error processing folder: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)