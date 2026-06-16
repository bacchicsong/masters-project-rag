"""
Скрипт дообучения би-энкодера на триплетах из фидбек-данных.

Использует TripletLoss для обучения различия между
релевантными (positive) и нерелевантными (negative) документами.

Запуск:
    python -m src.tools.fine_tune_bi_encoder

Или из корня проекта:
    python src/tools/fine_tune_bi_encoder.py

Вывод: модель в ./models/fine_tuned_bi_encoder/
"""

import sys
from pathlib import Path

# Add src to path so we can import infrastructure modules
SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))

from sentence_transformers import (
    SentenceTransformer,
    InputExample,
    losses,
    evaluation,
)
from torch.utils.data import DataLoader

from infrastructure.feedback.feedback_storage import FeedbackStorage

EMBEDDING_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
OUTPUT_PATH = str(Path(__file__).parent.parent.parent / "models" / "fine_tuned_bi_encoder")
BATCH_SIZE = 32
EPOCHS = 3
WARMUP_STEPS = 100
MIN_TRIPLETS = 10


def load_triplets() -> list:
    """Загружает триплеты из feedback хранилища и возвращает InputExample для обучения."""
    storage = FeedbackStorage()
    records = storage.load_all()
    print(f"Загружено {len(records)} триплетов из хранилища.")

    if len(records) < MIN_TRIPLETS:
        print(f"Недостаточно данных для обучения (минимум {MIN_TRIPLETS}). Пропускаем.")
        return []

    examples = []
    for r in records:
        # Positive pair: query близок к positive_doc
        examples.append(
            InputExample(texts=[r.query, r.positive_doc], label=1.0)
        )
        # Negative pair: query далёк от negative_doc
        examples.append(
            InputExample(texts=[r.query, r.negative_doc], label=0.0)
        )

    print(f"Создано {len(examples)} примеров для обучения.")
    return examples


def fine_tune():
    """Запускает процесс дообучения би-энкодера."""
    examples = load_triplets()
    if not examples:
        return {"status": "skipped", "reason": "not_enough_triplets", "examples": 0}

    # Разделяем на train/eval (90/10)
    split = int(len(examples) * 0.9)
    train_examples = examples[:split]
    eval_examples = examples[split:]

    print(f"Train: {len(train_examples)}, Eval: {len(eval_examples)}")

    # Загружаем модель
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    print(f"Загружена базовая модель: {EMBEDDING_MODEL_NAME}")

    # Создаём dataloader
    train_dataloader = DataLoader(
        train_examples,
        shuffle=True,
        batch_size=BATCH_SIZE,
    )

    # Loss: CosineSimilarityLoss — сближает похожие пары, отдаляет непохожие
    train_loss = losses.CosineSimilarityLoss(model)

    # Evaluator
    evaluator = evaluation.EmbeddingSimilarityEvaluator.from_input_examples(
        eval_examples,
        name="eval",
        batch_size=BATCH_SIZE,
    )

    # Обучаем
    print(f"\nНачало обучения: {EPOCHS} эпох, batch_size={BATCH_SIZE}")
    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        evaluator=evaluator,
        epochs=EPOCHS,
        warmup_steps=WARMUP_STEPS,
        evaluation_steps=100,
        output_path=OUTPUT_PATH,
        save_best_model=True,
        show_progress_bar=True,
    )

    print(f"\n✅ Модель сохранена в: {OUTPUT_PATH}")
    print("Для использования обновите EMBEDDING_MODEL_NAME в src/infrastructure/db/qdrand.py")
    return {
        "status": "trained",
        "examples": len(examples),
        "train_examples": len(train_examples),
        "eval_examples": len(eval_examples),
        "output_path": OUTPUT_PATH,
    }


if __name__ == "__main__":
    fine_tune()
