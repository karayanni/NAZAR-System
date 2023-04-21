import time


def lambda_handler(event, context):

    explanation_attributes = ['weather', 'location', 'id', 'model_type']
    start_time = time.time()

    print(explanation_attributes)

    return (time.time() - start_time), explanation_attributes
