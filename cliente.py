from kafka import KafkaConsumer
import sys

TOPICO_ALUGAR = 'alugar'
TOPICO_VENDER = 'vender'

# Pega a opção do usuário
escolha = sys.argv[1]

if escolha == '1':
    topicos = [TOPICO_ALUGAR]
elif escolha == '2':
    topicos = [TOPICO_VENDER]
elif escolha == '3':
    topicos = [TOPICO_ALUGAR, TOPICO_VENDER]
else:
    print("Opção inválida!")
    print("1 - Alugar")
    print("2 - Comprar")
    print("3 - Alugar e Comprar")
    print("ex: python3 cliente.py 1")
    sys.exit(1)

consumer = KafkaConsumer(*topicos)

for message in consumer:
    topic = message.topic.upper()
    endereco = message.value.decode('utf-8')
    imobiliaria = message.key.decode('utf-8')

    print(f"{imobiliaria}: {topic} {endereco}")
