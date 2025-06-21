class FluxoFile:
    def __init__(self, line, permitir_ipv6=False):
        # Estrutura da linha:
        # <IP1>:<Porta1> <-> <IP2>:<Porta2> <PacotesIP1> <BytesIP1> <sizeIP1> <PacotesIP2> <BytesIP2> <sizeIP2> <PacotesTotal3> <BytesTotal3> <sizeTotal3> <Inicio> <Duracao>
        # Ex:
        # 23.36.44.166:443 <-> 163.33.141.15:52079          0 0 bytes      36136 2385012 bytes      36136 2385012 bytes 0,000000  71,916941
        data = line.split() # Separa a linha em uma lista de strings

        if not permitir_ipv6:
            # Verifica se a linha tem ipv6, se tiver ativa uma flag
            if len(data[0].split(":")) > 2:
                self.ipv6 = True
                return
            else:
                self.ipv6 = False

        self.src = self.ip_to_int(data[0].split(":")[0]) if not permitir_ipv6 else self.get_ip(data[0])
        self.src_port = int(data[0].split(":")[1]) if not permitir_ipv6 else int(data[0].split(":")[-1])
        self.dst = self.ip_to_int(data[2].split(":")[0]) if not permitir_ipv6 else self.get_ip(data[2])
        self.dst_port = int(data[2].split(":")[1]) if not permitir_ipv6 else int(data[2].split(":")[-1])
        self.npackets_src = int(data[3])
        self.nbytes_src = int(data[4])
        self.size_src = data[5]
        self.npackets_dst = int(data[6])
        self.nbytes_dst = int(data[7])
        self.size_dst = data[8]
        self.npackets_total = int(data[9])
        self.nbytes_total = int(data[10])
        self.size_total = data[11]
        self.start = self.time_to_milliseconds(data[12])
        self.duration = self.time_to_milliseconds(data[13])

        # Converte os bytes para o tamanho correto
        self.nbytes_src = self.adjust_bytes(int(self.nbytes_src), self.size_src)
        self.nbytes_dst = self.adjust_bytes(int(self.nbytes_dst), self.size_dst)
        self.nbytes_total = self.adjust_bytes(int(self.nbytes_total), self.size_total)

    def adjust_bytes(self, bytes, size):
        if size == "bytes":
            return bytes
        if size == "kB":
            return bytes * 1024
        if size == "MB":
            return bytes * 1024 * 1024
        if size == "GB":
            return bytes * 1024 * 1024 * 1024
        if size == "TB":
            return bytes * 1024 * 1024 * 1024 * 1024
        
        return bytes
    
    # Converte o tempo em milissegundos
    def time_to_milliseconds(self, time):
        return int(float(time.replace(",", ".")) * 1000)

    # Converte o ip para inteiro
    def ip_to_int(self, ip):
        return int(''.join([f'{int(num):08b}' for num in ip.split('.')]), 2)
    
    # IP port to IP String
    def get_ip(self, data):
        port = data.split(":")[-1]
        # IP tudo menos a porta e os dois pontos
        ip = data[:-len(port) - 1]
        return ip
    
    # Converte o objeto para um dicionário
    def to_dict(self):
        return {
            "src": self.src,
            "src_port": self.src_port,
            "dst": self.dst,
            "dst_port": self.dst_port,
            "npackets_src": self.npackets_src,
            "nbytes_src": self.nbytes_src,
            "npackets_dst": self.npackets_dst,
            "nbytes_dst": self.nbytes_dst,
            "npackets_total": self.npackets_total,
            "nbytes_total": self.nbytes_total,
            "start": self.start,
            "duration": self.duration
        }
    
    def __str__(self):
        # Printa cada atributo do objeto em uma linha
        return "\n".join([f"{key}: {value}" for key, value in self.__dict__.items()])



