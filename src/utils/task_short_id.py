import hashlib


def generate_short_id(full_task_id: str, user_id: str, length: int = 3) -> str:
    combined = f"{full_task_id}:{user_id}".encode()
    hash_bytes = hashlib.blake2b(combined, digest_size=4).digest()
    hash_num = int.from_bytes(hash_bytes, byteorder='big')

    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []
    for _ in range(length):
        hash_num, remainder = divmod(hash_num, 36)
        result.append(chars[remainder])
    return ''.join(reversed(result))
