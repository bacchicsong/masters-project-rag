from typing import List, Dict, Set, Union

def calculate_precision_at_k(actual: List[Union[int, str]], predicted: List[Union[int, str]], k: int) -> float:
 
    if k <= 0:
        return 0.0
    
    predicted_k = predicted[:k]
    
    actual_set = set(actual)
    
    relevant_count = 0
    for p in predicted_k:
        if p in actual_set:
            relevant_count += 1
            
    return relevant_count / k

def calculate_recall_at_k(actual: List[Union[int, str]], predicted: List[Union[int, str]], k: int) -> float:
    
    if not actual:
        return 0.0 
    
    if k <= 0:
        return 0.0
        
    predicted_k = predicted[:k]
    actual_set = set(actual)
    
    relevant_count = 0
    for p in predicted_k:
        if p in actual_set:
            relevant_count += 1
            
    return relevant_count / len(actual_set)

def evaluate_system(ground_truth: Dict[str, List[str]], predictions: Dict[str, List[str]], k: int):
   
    
    total_precision = 0.0
    total_recall = 0.0
    count = 0
    
    for q_id in ground_truth:
        if q_id in predictions:
            actual_docs = ground_truth[q_id]
            predicted_docs = predictions[q_id]
            
            p_at_k = calculate_precision_at_k(actual_docs, predicted_docs, k)
            r_at_k = calculate_recall_at_k(actual_docs, predicted_docs, k)
            
            total_precision += p_at_k
            total_recall += r_at_k
            count += 1
            
    if count == 0:
        return 0.0, 0.0
        
    avg_precision = total_precision / count
    avg_recall = total_recall / count
    
    return avg_precision, avg_recall

# if __name__ == "__main__":
#     ground_truth_data = {
#         'q1': ['article_A', 'article_B'], 
#         'q2': ['article_C'],            
#         'q3': ['article_X', 'article_Y', 'article_Z'] 
#     }

#     model_predictions = {
#         'q1': ['article_B', 'article_F', 'article_A', 'article_G'], 
#         'q2': ['article_D', 'article_E', 'article_C'],              
#         'q3': ['article_M', 'article_N', 'article_X']              
#     }

#     K_VALUES = [1, 3, 5]

#     print(f"{'K':<5} | {'Mean Precision@k':<20} | {'Mean Recall@k':<20}")
#     print("-" * 50)

#     for k in K_VALUES:
#         avg_p, avg_r = evaluate_system(ground_truth_data, model_predictions, k)
#         print(f"{k:<5} | {avg_p:<20.4f} | {avg_r:<20.4f}")