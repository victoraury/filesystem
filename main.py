import mmap
import datetime


DISKSIZE = 128*(2**20)
BLOCKSIZE = 4*(2**10)
BLOCKNUMBER = int(DISKSIZE/BLOCKSIZE)


# iNode
# nome -> 128B
# tipo -> 2B (uint 16 - 0:dir, 1:file)
# dono -> 30B (nome do dono)
# criado -> 4B (unsigned int timestamp)
# modificado -> 4B (unsigned int timestamp)
# 168 bytes até aqui
 
# ponteiros -> 2B (uint 16 apontando para blocos)
# (4096-168)/2 = 1962 max blocos referenciados
class iNode:

    def __init__(self, name, itype, created, modified, owner, table = []):
        self.name = name
        self.type = itype
        self.created = int(created)
        self.modified = int(modified)
        self.owner = owner
        self.table = table              # nao pode conter zeros

    def __repr__(self) -> str:
        return f"({self.name}, {self.owner}, {self.created}, {self.modified}, {self.table})"
    
    def toBytes(self):
        serialized = bytearray(BLOCKSIZE)
        index = 0
        # nome do arquivo/diretorio
        name = bytearray(self.name, encoding='utf-8')
        if len(name) > 128:
            raise Exception(f"Erro: o nome \"{self.name}\" possui um tamanho maior do que o máximo permitido")
        serialized[index: index+len(name)] = name
        index += 128
        
        # tipo
        serialized[index:index+2] = int.to_bytes(self.type, 2, 'big', signed=False)
        index += 2
        # criado
        serialized[index:index+4] = int.to_bytes(self.created, 4, 'big', signed=False)
        index += 4
        # modificado
        serialized[index:index+4] = int.to_bytes(self.modified, 4, 'big', signed=False)
        index += 4
        
        # dono
        ow = bytearray(self.owner, 'utf-8')
        if len(ow) > 30:
            raise Exception(f"Erro: o nome do dono \"{self.owner}\" possui um tamanho maior do que o máximo permitido")
        serialized[index:index+len(ow)] = ow
        index += 30

        # tabela de blocos
        blocks = bytearray()
        if len(self.table) > 1962:
            raise Exception(f"Erro: tamanho máximo de referências excedido")
        for block in self.table:
            blocks.extend(int.to_bytes(block, 2, 'big', signed=False))
        null = int.to_bytes(65535, 2, 'big', signed=False)
        for i in range(len(self.table), 3924):
            blocks.extend(null)
        serialized[index: index+3924] = blocks
        index += 3924

        return serialized
    
    @staticmethod
    def fromBytes(byteblock):
        blocks = [int.from_bytes(byteblock[i:i+2], 'big', signed=False) for i in range(168, 4096, 2)]
        return iNode(
            byteblock[0:128].decode('utf-8'),
            int.from_bytes(byteblock[128:130], 'big', signed=False),
            int.from_bytes(byteblock[130:134], 'big', signed=False),
            int.from_bytes(byteblock[134:138], 'big', signed=False),
            byteblock[138:168].decode('utf-8'),
            [block for block in blocks if block != 65535]
        )

class DiskManager:

    def __init__(self, diskpath) -> None:
        d = open(diskpath, 'r+b')
        self.disk = mmap.mmap(d.fileno(), 0)

    def _readBytes(self, start, end=None):
        # lê do disco os bytes no intervalo "start":"end"
        # se end nao for passado, lê apenas 1 byte
        if end is None:
            return self.disk[start]
        else:
            return self.disk[start:end]
    
    def _writeBytes(self, atIndex, bytes):
        # escreve "bytes" no disco a partir do byte "atIndex"
        self.disk[atIndex: atIndex+len(bytes)] = bytes
        self.disk.flush(atIndex, len(bytes))
    
    @staticmethod
    def _blockify(bytes):
        # transforma um bytearray de tamanho n "bytes" em uma lista de bytearray's de tamanho BLOCKSIZE
        # se a divisão não for exata, faz padding
        blocks = []
        for i in range(0, len(bytes), BLOCKSIZE):
            b = bytes[i:i+BLOCKSIZE]
            blocks.append(b)

        if len(blocks) and len(blocks[-1]) != BLOCKSIZE:
            blocks[-1].extend(bytearray(BLOCKSIZE - len(blocks[-1])))

        return blocks


def resetDisk():
    bytearr = bytearray(DISKSIZE)
    with open('disk.bin', 'wb') as disk:
        disk.write(bytearr)

def test():
    a = iNode('a', 1, 50, 90, 'eu', [1,2,3])
    a_b = a.toBytes()
    b = iNode.fromBytes(a_b)

    print(a,b)
    pass


if __name__ == "__main__":
    # resetDisk()
    test()



    pass

