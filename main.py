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
        
        # nome do arquivo/diretorio
        name = bytearray(self.name, encoding='utf-8')
        if len(name) > 128:
            raise Exception()
        serialized[0:len(name)] = name
        
        # tipo
        serialized[128:130] = int.to_bytes(self.type, 2, 'big', signed=False)
        # criado
        serialized[130:134] = int.to_bytes(self.created, 4, 'big', signed=False)
        # modificado
        serialized[134:138] = int.to_bytes(self.modified, 4, 'big', signed=False)
        
        # dono
        ow = bytearray(self.owner, 'utf-8')
        if len(ow) > 30:
            raise Exception()
        serialized[138:138+len(ow)] = ow

        # tabela de blocos
        blocks = bytearray()
        if len(self.table) > 1962:
            raise Exception()
        for block in self.table:
            blocks.extend(int.to_bytes(block, 2, 'big', signed=False))
        serialized[168:168+len(blocks)] = blocks

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
            [block for block in blocks if block != 0]
        )

class DiskManager:

    def __init__(self, diskpath) -> None:
        d = open(diskpath, 'r+b')
        self.disk = mmap.mmap(d.fileno(), 0)
    
    def _writeBlock(self, content, block):
        pass
    
    @staticmethod
    def _blockify(bytes):
        
        blocks = []
        for i in range(0, len(bytes), BLOCKSIZE):
            b = bytes[i:i+BLOCKSIZE]
            blocks.append(b)

        if len(blocks) and len(blocks[-1]) != BLOCKSIZE:
            blocks[-1].extend(bytearray(BLOCKSIZE - len(blocks[-1])))

        return blocks

    def _readBytes(self, start, end=None):
        if end is None:
            return self.disk[start]
        else:
            return self.disk[start:end]

def resetDisk():
    bytearr = bytearray(DISKSIZE)
    with open('disk.bin', 'wb') as disk:
        disk.write(bytearr)

def test():
    # original = iNode(
    #     'root',
    #     1,
    #     datetime.datetime.now().timestamp(),
    #     datetime.datetime.now().timestamp()+1,
    #     'victor',
    #     [i for i in range(2,30)]
    # )
    # og_bytes = original.toBytes()
    # lido = iNode.fromBytes(og_bytes)
    # ld_bytes = lido.toBytes()

    # print(og_bytes == ld_bytes)
    # print(original, lido)

    yo = bytearray()
    [yo.extend(i.encode('utf-8')) for i in ['kekw', 'yo']]
    
    print(yo)
    print(DiskManager._blockify(yo))
    



def main():
    # print(DISKSIZE, BLOCKNUMBER)

    disk = open('disk.bin', 'r+b')
    A = mmap.mmap(disk.fileno(), 0)

    print(A[0:10])


if __name__ == "__main__":
    # resetDisk()
    # main()
    test()