import hashlib
import socket
import threading
import os
import time

CURRENT_FOLDER = os.getcwd()
PATH = f'{CURRENT_FOLDER}/shared'
BORDER_NODE_IP = "192.168.0.154"
HANDSHAKE_PASSED=False
lock = threading.Lock()

def calculate_checksum(filename):
    hasher = hashlib.md5()
    with open(f"shared/{filename}", 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def list_files_in_folder():
    files_info = []
    if not os.path.exists(PATH):
        try:
            os.makedirs(PATH)  # Tenta criar o diretório se não existir
            print(f"Pasta '{PATH}' criada com sucesso.")
        except OSError as e:
            print(f"Erro ao criar pasta '{PATH}': {e}")
            return []

    for root, _, filenames in os.walk(PATH):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            size = os.path.getsize(file_path)
            checksum = calculate_checksum(filename)
            files_info.append({
                'filename': filename,
                'checksum': checksum,
                'size': size
            })
    return files_info

def handle_border_node(client_socket):
    while True:
        try:
            client_socket.send(str(list_files_in_folder()).encode())
            # client_socket.send(('Hello').encode())
            client_socket.close()
            break
        except ConnectionResetError:
            break

def handle_regular_node(client_socket):
    filename = client_socket.recv(1024).decode()
    file_path = os.path.join(PATH, filename)

    try:
        with open(file_path, 'rb') as arquivo:
            print(f"Enviando arquivo {filename} para o cliente...")

            while True:
                chunk = arquivo.read(4096)
                if not chunk:
                    break

                # Verifica se a conexão ainda está ativa antes de enviar dados
                if client_socket.fileno() == -1:
                    print("Conexão fechada pelo cliente.")
                    break

                client_socket.send(chunk)

            print("Arquivo enviado com sucesso!")

            # Calcula o checksum do arquivo e envia para o cliente
            checksum = calculate_checksum(filename)
            client_socket.send(str(checksum).encode())

    except FileNotFoundError:
        print(f"Arquivo {filename} não encontrado.")
        client_socket.send("FileNotFound".encode())

    except (BrokenPipeError, ConnectionResetError):
        print("Erro de conexão durante a transmissão.")
        # Lida com a exceção de conexão interrompida de forma adequada

    except Exception as e:
        print(f"Erro ao enviar arquivo {filename}: {e}")
        client_socket.send("ServerError".encode())

    finally:
        # Fecha o arquivo e o socket
        arquivo.close()
        client_socket.close()

    


def node_server():
    global HANDSHAKE_PASSED
    #Esse server e somente para se comunicar com os outros nos
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("0.0.0.0", 9998))
    listener.listen(4)
    print('Aberto a conexoes...')
    while True:
        client_socket, addr = listener.accept()
        if addr[0] == BORDER_NODE_IP:
            client_handler = threading.Thread(target=handle_border_node, args=(client_socket,))
            client_handler.start()
        else:
            threading.Thread(target=handle_regular_node, args=(client_socket,)).start()


def main():
    global HANDSHAKE_PASSED
    threading.Thread(target=node_server, args=()).start()
    time.sleep(3)
    #Conectando ao no de borda
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((BORDER_NODE_IP, 9999))
    
    client.send(('handshake').encode())
    start_time = time.time()
    while True:
        if client.recv(1024).decode() == 'ack':
            with lock:
                HANDSHAKE_PASSED = True
            break
        elif time.time() - start_time > 15:
            print('Timeout! Encerrando conexao')
            client.close()
            exit(1)
    
    while True:
        print('1- Buscar arquivo\n2- Fechar conexao com no de borda')    
        opt = int(input())
        if opt == 1:
            filename = str(input('Qual arquivo deseja buscar? '))
            client.send(str(['getfiles', filename]).encode())
            lista_dados = eval(client.recv(1024).decode())
            ip = lista_dados[0]
            checksum = lista_dados[1]
            threading.Thread(target=get_file_from_regular_node, args=(ip, filename, checksum)).start()
            print('ip: '+ip)
        else:
            print('Encerrando conexao')
            exit(1)

def get_file_from_regular_node(ip, filename, true_checksum):
    print(f'Abrindo conexao com: {ip}; filename: {filename}')
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((ip, 9998))
    client.send(filename.encode())
    arquivo = open(f"shared/{filename}",'wb')

    # Le os dados
    while True:

        # Recebe os dados do arquivo
        dados = client.recv(4096)

        # Verifica se acabou a transferencia
        if not dados:
            break

        # Escreve os dados do arquivo
        arquivo.write(dados)
    arquivo.close()
    checksum_recived = client.recv(1024).decode('utf-8')

    checksum_local = calculate_checksum(filename)
    if checksum_local == checksum_recived:
        print('Arquivo integro')
    else:
        print('Arquivo corrompido')
    client.close()

if __name__ == "__main__":
    main()