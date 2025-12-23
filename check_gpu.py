"""
=============================================================================
check_gpu.py - Скрипт проверки доступности GPU
=============================================================================

Этот скрипт проверяет наличие CUDA-совместимого GPU и выводит информацию
о доступных вычислительных устройствах.

Использование:
    python check_gpu.py

Автор: Команда Atomichack 3.0
=============================================================================
"""

import sys
import torch
from processing.ml_analysis import get_device

# Устанавливаем UTF-8 для корректного отображения эмодзи в Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def main():
    """
    Проверяет доступность GPU и выводит детальную информацию.
    """
    print("=" * 80)
    print("🔍 ПРОВЕРКА ДОСТУПНОСТИ GPU")
    print("=" * 80)
    print()
    
    # Информация о PyTorch
    print(f"📦 Версия PyTorch: {torch.__version__}")
    print(f"🔧 CUDA доступна: {'Да ✅' if torch.cuda.is_available() else 'Нет ❌'}")
    print()
    
    # Определение устройства
    print("-" * 80)
    print("🎯 Определение оптимального устройства:")
    print("-" * 80)
    device = get_device()
    print()
    
    # Детальная информация о GPU (если доступен)
    if torch.cuda.is_available():
        print("-" * 80)
        print("💎 ИНФОРМАЦИЯ О GPU:")
        print("-" * 80)
        gpu_count = torch.cuda.device_count()
        print(f"   Количество GPU: {gpu_count}")
        print()
        
        for i in range(gpu_count):
            props = torch.cuda.get_device_properties(i)
            print(f"   GPU {i}: {torch.cuda.get_device_name(i)}")
            print(f"      • Память: {props.total_memory / 1024**3:.2f} GB")
            print(f"      • Compute Capability: {props.major}.{props.minor}")
            print(f"      • Мультипроцессоры: {props.multi_processor_count}")
            print()
        
        # Версия CUDA
        print(f"   CUDA версия: {torch.version.cuda}")
        print(f"   cuDNN версия: {torch.backends.cudnn.version()}")
        print()
        
        print("=" * 80)
        print("✅ GPU ДОСТУПЕН! Приложение будет работать на GPU.")
        print("   Ожидаемое ускорение: 5-10x по сравнению с CPU")
        print("=" * 80)
    else:
        print("-" * 80)
        print("💡 РЕКОМЕНДАЦИИ ДЛЯ ВКЛЮЧЕНИЯ GPU:")
        print("-" * 80)
        print("   1. Убедитесь, что у вас есть NVIDIA GPU")
        print("   2. Установите драйверы NVIDIA: https://www.nvidia.com/drivers")
        print("   3. Установите CUDA Toolkit: https://developer.nvidia.com/cuda-downloads")
        print("   4. Установите PyTorch с CUDA поддержкой:")
        print("      pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118")
        print()
        print("=" * 80)
        print("ℹ️  GPU НЕДОСТУПЕН. Приложение будет работать на CPU.")
        print("   Это нормально, но обработка будет медленнее.")
        print("=" * 80)
    
    print()


if __name__ == "__main__":
    main()

