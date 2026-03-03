mport pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_cdr(num_records=1000):
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 3, 1)
    
    callers = [f"+1555010{i:03d}" for i in range(100)]
    callees = [f"+1555020{i:03d}" for i in range(100)]
    results = ['connected', 'failed', 'busy', 'no_answer']
    
    data = []
    for i in range(num_records):
        caller = random.choice(callers)
        callee = random.choice(callees)
        
        # Introduce some patterns:
        # Caller +1555010001 is a frequent caller
        if random.random() < 0.1:
            caller = "+1555010001"
            
        # Introduce a "burst" fraud pattern (one caller makes many calls in a very short window)
        if i > 950:
            caller = "+1555010999" # The fraudster
            timestamp = start_date + timedelta(days=10, hours=10, minutes=random.randint(0, 5))
            duration = random.randint(1, 5) # Very short calls
            result = random.choice(['connected', 'failed'])
        else:
            timestamp = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            # Duration: log-normal or similar? Most calls short, some long.
            duration = int(random.expovariate(1/120)) # avg 2min
            if random.random() < 0.05: # some very long calls
                duration = random.randint(3600, 7200)
            result = random.choices(results, weights=[0.8, 0.05, 0.1, 0.05])[0]
        
        data.append({
            'record_id': f"REC_{i:06d}",
            'caller': caller,
            'callee': callee,
            'timestamp': timestamp.isoformat(),
            'duration': duration,
            'result': result
        })
        
    df = pd.DataFrame(data)
    df.to_csv('sample_cdr.csv', index=False)
    print(f"Generated {num_records} CDR records to sample_cdr.csv")

if __name__ == "__main__":
    generate_cdr()
