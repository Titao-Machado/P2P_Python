import socket
import threading
import time

nodes = {}
lock = threading.Lock()

def att_node_list():
    while True:
        if len(nodes) == 0:
            print('Sem nos conectados...')
        else:
            for files in nodes.items():
                try:

                    print('Tentando acessar lista de arquivos dos nos regulares atualizadas...')
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect((files[0], 9998))
                    new_list = client.recv(4096).decode()
                    nodes[files[0]] = eval(new_list)
                    client.close()
                    print("Sucesso!")
                except:
                    print('Conexao do no regular encerrada! Removendo arquvos disponiveis...')
                    nodes.pop(files[0])
                    if len(nodes) == 0:
                        break

        time.sleep(20)

def search_file_in_nodes(client_socket, search_file):
    node_ip = ''
    for ip, file_list in nodes.items():
        for file_info in file_list:
            if file_info['filename'] == search_file:
                node_ip = str(ip)
                info = [node_ip, file_info['checksum']]
                client_socket.send(str(info).encode())
    if node_ip == '':
        client_socket.send(('Nenhum no conectado possui o arquivo digitado').encode())

def get_files_by_name(client_socket, search_file):
    print('Starting getfiles protocol')
    print('filename: '+search_file)
    search_file_in_nodes(client_socket, search_file)

def get_files(client_socket, ip, handshake = False):
    print('Starting handshake protocol')
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((ip, 9998))
    if handshake == True:
        data = client.recv(4096).decode()
        data_json = eval(data)
        with lock:
            nodes[ip] = nodes.get(ip, [])
            nodes[ip].clear()
            for d in data_json:
                nodes[ip].append(d)
            print(nodes)
        client_socket.send(('ack').encode())
    else:
        data = client.recv(4096).decode()
        data_json = eval(data)
        with lock:
            for d in data_json:
                nodes[ip] = nodes.get(ip, [])
                nodes[ip].append(d)
                print(nodes)
    client.close()

def handle_client(client_socket, ip):
    while True:
        try:
            # Recebe a mensagem do cliente
            request = client_socket.recv(1024).decode('utf-8')
            if request == 'handshake':
                threading.Thread(target=get_files, args=(client_socket, ip, True)).start()
            else:
                protocol = eval(request)[0]
                filename = eval(request)[1]
            
                threading.Thread(target=get_files_by_name, args=(client_socket, filename)).start()
        except ConnectionResetError:
            break

    client_socket.close()
    #Todo logica para remover arquivos do ip
    for files in nodes.items():
        nodes.pop(ip)
        if len(nodes) == 0:
            break
    print('Lista atualizada '+str(nodes))
    print("Conexão fechada com o cliente.")

def main():
    threading.Thread(target=att_node_list, args=()).start()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 9999))
    server.listen(5)
    print("Servidor ouvindo na porta 9999...")

    while True:
        client_socket, addr = server.accept()
        print(f"Conexão aceita de: {addr}")

        client_handler = threading.Thread(target=handle_client, args=(client_socket, addr[0]))
        client_handler.start()
if __name__ == "__main__":
    main()