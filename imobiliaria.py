from kafka import KafkaProducer
import pandas as pd
import random 
from time import sleep
import sys

TOPICO_ALUGAR = 'alugar'
TOPICO_VENDER = 'vender'

df = pd.read_csv('curitiba_logradouros.csv', sep=';')

# Pega o nome da imobiliária
nome = ' '.join(sys.argv[1:])
if not nome:
    print("Informe um nome para a imobiliária")
    print("ex: python3 imobiliaria.py Imobiliária do João")
    sys.exit(1)

producer = KafkaProducer()

while True:
    sleep(random.randint(5, 15))

    # Escolhe um tipo de imóvel aleatório
    tipo = random.choice(['alugar', 'vender'])
    # Escolhe um logradouro aleatório
    logradouro = random.choice(df['logradouro'])

    if tipo == 'alugar':
        print(f"{nome}: ALUGAR {logradouro}")
        topico = TOPICO_ALUGAR
    else:
        print(f"{nome}: VENDER {logradouro}")
        topico = TOPICO_VENDER
    
    producer.send(topico, logradouro.encode('utf-8'), key=nome.encode('utf-8'))

producer.flush()