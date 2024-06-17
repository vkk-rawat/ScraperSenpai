import os
import hashlib
import logging
import requests
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from bs4 import BeautifulSoup

# SQLAlchemy setup
Base = declarative_base()


class PDF(Base):
    __tablename__ = 'pdfs'
    id = Column(Integer, primary_key=True)
    filename = Column(String, unique=True)
    checksum = Column(String, unique=True)


engine = create_engine('sqlite:///pdf_tracker.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Logging setup
logging.basicConfig(filename='scraper.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Functions


def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def download_pdf(pdf_url, save_path):
    response = requests.get(pdf_url)
    with open(save_path, 'wb') as file:
        file.write(response.content)

# Main script logic


def main():
    try:
        url = 'http://ggsipu.ac.in/ExamResults/ExamResultsmain.htm'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        pdf_links = [a['href'] for a in soup.find_all(
            'a', href=True) if a['href'].endswith('.pdf')]

        for pdf_link in pdf_links:
            pdf_url = os.path.join(url, pdf_link)
            save_path = os.path.join('pdfs', os.path.basename(pdf_link))
            download_pdf(pdf_url, save_path)

            checksum = calculate_md5(save_path)
            existing_pdf = session.query(PDF).filter_by(
                checksum=checksum).first()

            if not existing_pdf:
                new_pdf = PDF(filename=os.path.basename(
                    pdf_link), checksum=checksum)
                session.add(new_pdf)
                session.commit()
                logging.info(f"PDF downloaded and stored: {save_path}")
            else:
                os.remove(save_path)
                logging.info(f"Duplicate PDF found and removed: {save_path}")
    except Exception as e:
        logging.error('Error occurred: %s', e)


if __name__ == '__main__':
    main()
