import mysql.connector as sql, os, csv, json, traceback, logging
from telegram.ext import *
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

#Logger
logging.basicConfig(filename=r"log/bot.log",
                    format="%(asctime)s in %(funcName)s\n  %(message)s")

#Google Drive
scopes = ["https://www.googleapis.com/auth/drive"]
path_token = r"src/token.json"
folder = "[FOLDER_ID]"

#Telegram
userid = ["[USER_ID]"]
token = "[TOKEN_BOT]"

#Database
host = "localhost"
user = "[USER_DB]"
password = "[PASS_DB]"
database = "[DB]"

#Verifikasi User
def verify(user):
    global userid
    #Memuat Data User dari JSON
    with open(r"src/userid.json", "r") as file:
        try:
            userid = json.load(file) 
        except:
            #Error
            logging.warning(traceback.format_exc().replace("\n", "\n  "))
    if str(user) in userid:
        return False
    else:
        return True

#Melihat Data Secara Realtime
async def show_data(update, context):
    #Verifikasi User
    if verify(update.message.from_user.id):
        await update.message.reply_text(
            "Mohon maaf, User ID %s milik Anda tidak terdaftar dalam sistem kami."\
            % update.message.from_user.id)
        return None
    try:
        #Koneksi ke Database
        db = sql.connect(
            host = host,
            user = user,
            password = password,
            database = database)
        cursor = db.cursor()
        #Mengeksekusi Query
        cursor.execute("SELECT * FROM monitoring_daya ORDER BY timestamp DESC LIMIT 1")
        sensor = tuple(cursor.fetchall()[0])[1:]
        db.close()
        #Mengirim Pesan ke User
        await update.message.reply_text(
            "Monitoring Daya Greenhouse\n"\
            " - Timestamp : %s\n"\
            " - Voltage PV : %s Volt\n"\
            " - Current PV : %s Ampere\n"\
            " - Power PV : %s Watt\n"\
            " - Voltage VAWT : %s Volt\n"\
            " - Current VAWT : %s Ampere\n"\
            " - Power VAWT : %s Watt\n"\
            " - Anemometer : %s m/s\n"\
            " - Voltage Battery : %s Volt"\
            % sensor)
    except Exception as error:
        #Error
        logging.warning(traceback.format_exc().replace("\n", "\n  "))
        await update.message.reply_text(
            "Mohon maaf, terjadi kesalahan sistem saat sedang memproses data. "\
            "Silahkan coba beberapa saat lagi.\nError : %s" % error)

#Mengakuisisi Data CSV Melalui Telegram
async def get_csv(update, context):
    #Verifikasi User
    if verify(update.message.from_user.id):
        await update.message.reply_text(
            "Mohon maaf, User ID %s milik Anda tidak terdaftar dalam sistem kami."\
            % update.message.from_user.id)
        return None
    try:
        #Memeriksa Format Perintah
        date = str(update.message.text).split("/get_csv ")[1]
        date_start = date.split(" ")[0]
        date_end = date.split(" ")[1]
        datetime.strptime(date_start, "%Y-%m-%d")
        datetime.strptime(date_end, "%Y-%m-%d")
    except:
        #Format Tidak Benar
        await update.message.reply_text(
            "Perintah yang dimasukkan tidak benar\nFormat :\n/get_csv YYYY-MM-DD YYYY-MM-DD\n"\
            "Contoh :\n/get_csv 2023-01-01 2023-01-20")
        return None
    try:
        await update.message.reply_text("Mohon menunggu, sistem sedang memproses data.")
        #Koneksi ke Database
        db = sql.connect(
            host = host,
            user = user,
            password = password,
            database = database)
        cursor = db.cursor()
        #Mengeksekusi Query
        cursor.execute(
            "SELECT timestamp, v_pv, i_pv, p_pv, v_vawt, i_vawt, p_vawt, anemo, v_bat "\
            "FROM monitoring_daya WHERE timestamp BETWEEN '%s 00:00:00' AND '%s 00:00:00' "\
            "ORDER BY id ASC" % (date_start, date_end))
        data = list(cursor.fetchall())
        db.close()
        #Menyimpan Data ke CSV
        column = ["Timestamp", "PV Voltage (Volt)", "PV Current (Ampere)", "PV Power (Watt)",\
                  "VAWT Voltage (Volt)", "VAWT Current (Ampere)", "VAWT Power (Watt)",\
                  "Anemometer (m/s)", "Battery Voltage (Volt)"]
        with open(r"cache_csv/Data Greenhouse %s to %s.csv" % (date_start, date_end), 
                  mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(column)
            writer.writerows(data)
            path = os.path.abspath(file.name)
        #Memeriksa Ukuran Berkas
        if os.path.getsize(path) >= 48000000:
            await update.message.reply_text("Mohon maaf, berkas csv melebihi 50 MB. "\
                                            "Silahkan menggunakan perintah /get_drive.")
        else:
            with open(path, "rb") as csv_file:
                await update.message.reply_document(
                    csv_file, read_timeout=120, write_timeout=120, connect_timeout=120)
    #Error
    except Exception as error:
        logging.warning(traceback.format_exc().replace("\n", "\n  "))
        await update.message.reply_text(
            "Mohon maaf, terjadi kesalahan sistem saat sedang memproses data. "\
            "Silahkan coba beberapa saat lagi.\nError : %s" % error)
    #Menghapus Berkas Cache
    try: os.remove(path)
    except: pass

#Mengakuisisi Data CSV Melalui Google Drive
async def get_drive(update, context):
    #Verifikasi User
    if verify(update.message.from_user.id):
        await update.message.reply_text(
            "Mohon maaf, User ID %s milik Anda tidak terdaftar dalam sistem kami."\
            % update.message.from_user.id)
        return None
    try:
        #Memeriksa Format Perintah
        date = str(update.message.text).split("/get_drive ")[1]
        date_start = date.split(" ")[0]
        date_end = date.split(" ")[1]
        datetime.strptime(date_start, "%Y-%m-%d")
        datetime.strptime(date_end, "%Y-%m-%d")
    except:
        #Format Tidak Benar
        await update.message.reply_text(
            "Perintah yang dimasukkan tidak benar\nFormat :\n/get_drive YYYY-MM-DD YYYY-MM-DD\n"\
            "Contoh :\n/get_drive 2023-01-01 2023-01-20")
        return None
    try:
        await update.message.reply_text("Mohon menunggu, sistem sedang memproses data.")
        #Koneksi ke Database
        db = sql.connect(
            host = host,
            user = user,
            password = password,
            database = database)
        cursor = db.cursor()
        #Mengeksekusi Query
        cursor.execute(
            "SELECT timestamp, v_pv, i_pv, p_pv, v_vawt, i_vawt, p_vawt, anemo, v_bat "\
            "FROM monitoring_daya WHERE timestamp BETWEEN '%s 00:00:00' AND '%s 00:00:00' "\
            "ORDER BY id ASC" % (date_start, date_end))
        data = list(cursor.fetchall())
        #Menyimpan Data ke CSV
        column = ["Timestamp", "PV Voltage (Volt)", "PV Current (Ampere)", "PV Power (Watt)",\
                  "VAWT Voltage (Volt)", "VAWT Current (Ampere)", "VAWT Power (Watt)",\
                  "Anemometer (m/s)", "Battery Voltage (Volt)"]
        with open(r"cache_drive/Data Greenhouse %s to %s.csv" % (date_start, date_end),
                  mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(column)
            writer.writerows(data)
            path = os.path.abspath(file.name)
        #Otorisasi Google Drive
        creds = Credentials.from_authorized_user_file(path_token, scopes)
        service = build("drive", "v3", credentials=creds)
        #Menggunggah Berkas
        file_metadata = {
            "name": "Data Greenhouse %s to %s.csv" % (date_start, date_end),
            "parents": [folder]}
        media = MediaFileUpload(
            path,
            mimetype="text/csv",
            resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink").execute()
        media.__del__()
        #Mengatur Akses Berkas
        permission = {
            "role": "reader",
            "type": "anyone"}
        service.permissions().create(
            fileId=file.get("id"),
            body=permission).execute()
        #Memasukkan fileId ke Database
        timestamp = datetime.now()
        cursor.execute("INSERT INTO google_drive (timestamp, file_id) VALUES ('%s', '%s')"\
            % (timestamp.strftime("%Y-%m-%d %H:%M:%S"), file.get("id")))
        db.commit()
        #Mengirim Tautan ke Telegram
        await update.message.reply_text("Berlaku sampai %s\n%s" % 
            ((timestamp + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
                file.get("webViewLink")))
    #Error
    except Exception as error:
        logging.warning(traceback.format_exc().replace("\n", "\n  "))
        await update.message.reply_text(
            "Mohon maaf, terjadi kesalahan sistem saat sedang memproses data. "\
            "Silahkan coba beberapa saat lagi.\nError : %s" % error)
    #Menghapus Berkas Cache
    try: os.remove(path)
    except: pass

#Mengecek Koneksi Database
async def check_db(update, context):
    #Verifikasi User
    if verify(update.message.from_user.id):
        await update.message.reply_text(
            "Mohon maaf, User ID %s milik Anda tidak terdaftar dalam sistem kami."\
            % update.message.from_user.id)
        return None
    try:
        #Koneksi ke Database
        db = sql.connect(
            host = host,
            user = user,
            password = password,
            database = database)
        db.close()
        await update.message.reply_text("Local database berhasil terhubung")
    except Exception as error:
        #Error
        logging.warning(traceback.format_exc().replace("\n", "\n  "))
        await update.message.reply_text("Local database tidak terhubung\nError : %s" % error)

#Handler untuk Error
async def error_handler(update, context):
    #Memasukkan Traceback Error ke Log
    traceback_string = "".join(traceback.format_exception(None,\
        context.error, context.error.__traceback__))
    logging.warning(traceback_string.replace("\n", "\n  "))

#Menghapus Berkas yang Lebih dari Seminggu
async def delete_drive(context):
    try:
        #Koneksi ke Database
        db = sql.connect(
            host = host,
            user = user,
            password = password,
            database = database
            )
        cursor = db.cursor()
        cursor.execute("SELECT * FROM google_drive")
        data = cursor.fetchall()
        #Iterasi fileId
        for datum in data:
            now = datetime.now()
            past = datum[1]
            #Mengecek Selisih Waktu Berkas
            if (now - past).days >= 7:
                try:
                    #Menghapus Berkas di Google Drive
                    creds = Credentials.from_authorized_user_file(path_token, scopes)
                    service = build("drive", "v3", credentials=creds)
                    service.files().delete(fileId=str(datum[2])).execute()
                    
                    #Menghapus Baris di Database
                    cursor.execute("DELETE FROM google_drive WHERE id = %s" % datum[0])
                    db.commit()
                except:
                    #Error
                    logging.warning(traceback.format_exc().replace("\n", "\n  "))
        db.close()
    #Error
    except:
        logging.warning(traceback.format_exc().replace("\n", "\n  "))

if __name__ == "__main__":
    #Membuat Objek dari class Application
    application = Application.builder().token(token).build()
    #Membuat Handler untuk Perintah
    application.add_handler(CommandHandler("check_db", check_db))
    application.add_handler(CommandHandler("show_data", show_data))
    application.add_handler(CommandHandler("get_csv", get_csv))
    application.add_handler(CommandHandler("get_drive", get_drive))
    #Membuat Handler untuk Error
    application.add_error_handler(error_handler)
    #Membuat Job Queue untuk Menghapus Berkas
    application.job_queue.run_repeating(delete_drive, interval=3600)
    #Menjalankan Bot
    application.run_polling()        
