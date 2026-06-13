#!/usr/bin/env python3
"""
Enhanced Provider Pattern Evaluation Script

Evaluates parser quality against golden samples and supports JSON output for automation.

Usage:
    python provider_pattern_eval.py [--verbose] [--json] [--provider PROVIDER] [--source SOURCE]
"""

import os
import sys
import json
import argparse
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from decimal import Decimal

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.etl.parsers.text_parser import parse_transaction

@dataclass
class EvaluationResult:
    """Single evaluation result"""
    sample_id: str
    provider: str
    source: str
    passed: bool
    errors: List[str]
    actual_output: Dict
    expected_output: Dict

@dataclass
class ProviderStats:
    """Statistics for a provider/source combination"""
    total_samples: int
    passed_samples: int
    pass_rate: float
    field_accuracy: Dict[str, float]
    direction_precision: float
    direction_recall: float
    direction_f1: float

@dataclass
class OverallStats:
    """Overall evaluation statistics"""
    total_samples: int
    passed_samples: int
    pass_rate: float
    provider_stats: Dict[str, Dict[str, ProviderStats]]

class ProviderPatternEvaluator:
    """Enhanced evaluation with JSON output support"""
    
    def __init__(self, golden_samples_path: str = "evaluation/ingestion/golden_samples.jsonl"):
        self.golden_samples_path = golden_samples_path
        self.tolerance = 0.01  # Amount tolerance for comparison
        
    def load_golden_samples(self, provider_filter: Optional[str] = None, 
                           source_filter: Optional[str] = None) -> List[Dict]:
        """Load golden samples with optional filtering"""
        samples = []
        
        try:
            with open(self.golden_samples_path, 'r', encoding='utf-8') as f:
                for line_no, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    
                    try:
                        sample = json.loads(line.strip())
                        
                        # Apply filters
                        if provider_filter and sample.get('provider') != provider_filter:
                            continue
                        if source_filter and sample.get('source') != source_filter:
                            continue
                        
                        samples.append(sample)
                        
                    except json.JSONDecodeError as e:
                        print(f"Warning: Invalid JSON on line {line_no}: {e}")
                        continue
                        
        except FileNotFoundError:
            print(f"Error: Golden samples file not found: {self.golden_samples_path}")
            return []
        
        return samples
    
    def _is_rejection_sample(self, sample: Dict) -> bool:
        return bool(sample.get('failure_reason') or sample.get('expect_parse_fail'))

    def _rejection_parse_ok(self, actual: Optional[Dict]) -> bool:
        if not actual:
            return True
        amount = actual.get('amount')
        if amount is None:
            return True
        parse_reason = str(actual.get('parse_reason') or '').lower()
        if parse_reason == 'failed':
            return True
        return False

    def evaluate_sample(self, sample: Dict) -> EvaluationResult:
        """Evaluate a single golden sample"""
        sample_id = sample.get('id') or sample.get('metadata', {}).get('staging_id', 'unknown')
        provider = sample.get('provider') or 'unknown'
        source = sample.get('source', 'unknown')
        raw_text = sample.get('raw_text', '')
        expected = sample.get('expected') or {}
        is_rejection = self._is_rejection_sample(sample)
        
        errors = []
        
        # Build parsing metadata
        meta = {}
        sender = (
            sample.get('sender_id')
            or sample.get('sender')
            or sample.get('sender_email')
        )
        if sender:
            if source == 'sms':
                meta['sender_id'] = sender
            elif source == 'gmail':
                meta['sender_email'] = sender
            elif source == 'notification':
                meta['sender_id'] = sender
        elif source == 'gmail' and sample.get('sender_email'):
            meta['sender_email'] = sample.get('sender_email')

        subject = (sample.get('subject') or '').strip()
        if subject:
            meta['subject'] = subject
            # Ingest combines title + body; mirror that when golden raw_text is body-only.
            if source == 'notification' and not raw_text.lstrip().startswith(subject):
                raw_text = f"{subject}\n{raw_text.strip()}".strip()
        
        # Parse the transaction
        try:
            actual = parse_transaction(raw_text, source=source, provider=provider, **meta)
        except Exception as e:
            return EvaluationResult(
                sample_id=sample_id,
                provider=provider,
                source=source,
                passed=False,
                errors=[f"Parsing failed: {str(e)}"],
                actual_output={},
                expected_output=expected
            )
        
        if not actual:
            if is_rejection:
                return EvaluationResult(
                    sample_id=sample_id,
                    provider=provider,
                    source=source,
                    passed=True,
                    errors=[],
                    actual_output={},
                    expected_output=expected
                )
            return EvaluationResult(
                sample_id=sample_id,
                provider=provider,
                source=source,
                passed=False,
                errors=["Parser returned None"],
                actual_output={},
                expected_output=expected
            )

        if is_rejection:
            ok = self._rejection_parse_ok(actual)
            return EvaluationResult(
                sample_id=sample_id,
                provider=provider,
                source=source,
                passed=ok,
                errors=[] if ok else ["Expected non-transaction but parser extracted amount"],
                actual_output=actual,
                expected_output=expected
            )
        
        # Compare fields
        errors = self._compare_fields(actual, expected)
        
        return EvaluationResult(
            sample_id=sample_id,
            provider=provider,
            source=source,
            passed=len(errors) == 0,
            errors=errors,
            actual_output=actual,
            expected_output=expected
        )
    
    def _compare_fields(self, actual: Dict, expected: Dict) -> List[str]:
        """Compare actual vs expected fields and return list of errors"""
        errors = []
        
        # Core fields that must match
        core_fields = ['txn_type', 'amount', 'date_str']
        
        for field in core_fields:
            if field not in expected:
                continue
                
            actual_value = actual.get(field)
            expected_value = expected[field]
            
            if not self._values_match(actual_value, expected_value, field):
                errors.append(f"{field}: expected='{expected_value}', actual='{actual_value}'")
        
        # Optional fields (only check if expected is not None)
        optional_fields = ['counterparty', 'account_ref', 'ref', 'time_str', 'fee', 
                          'description', 'balance', 'branch', 'pattern_name',
                          'beneficiary_account', 'sender_account', 'transfer_method']
        
        for field in optional_fields:
            expected_value = expected.get(field)
            if expected_value is not None:
                actual_value = actual.get(field)
                if not self._values_match(actual_value, expected_value, field):
                    errors.append(f"{field}: expected='{expected_value}', actual='{actual_value}'")
        
        return errors
    
    def _values_match(self, actual: Any, expected: Any, field_name: str) -> bool:
        """Check if two values match with appropriate tolerance"""
        
        # Handle None values
        if expected is None and actual is None:
            return True
        if expected is None or actual is None:
            return False
        
        if field_name == 'fee':
            try:
                a = 0.0 if actual is None else float(actual)
                e = 0.0 if expected is None else float(expected)
                return abs(a - e) <= self.tolerance
            except (ValueError, TypeError):
                return actual is None and expected is None

        if field_name in ('amount', 'balance'):
            try:
                actual_amount = float(actual) if actual is not None else 0.0
                expected_amount = float(expected) if expected is not None else 0.0
                return abs(actual_amount - expected_amount) <= self.tolerance
            except (ValueError, TypeError):
                return str(actual) == str(expected)
        
        # String comparison (case-insensitive for some fields)
        if field_name in ['counterparty', 'description']:
            return str(actual).strip().lower() == str(expected).strip().lower()
        
        # Exact match for other fields
        return str(actual).strip() == str(expected).strip()
    
    def calculate_stats(self, results: List[EvaluationResult]) -> OverallStats:
        """Calculate comprehensive evaluation statistics"""
        
        # Group results by provider and source
        provider_results = defaultdict(lambda: defaultdict(list))
        
        for result in results:
            provider_results[result.provider][result.source].append(result)
        
        # Calculate stats for each provider/source combination
        provider_stats = {}
        total_samples = 0
        total_passed = 0
        
        for provider, source_results in provider_results.items():
            provider_stats[provider] = {}
            
            for source, source_results_list in source_results.items():
                stats = self._calculate_provider_source_stats(source_results_list)
                provider_stats[provider][source] = stats
                
                total_samples += stats.total_samples
                total_passed += stats.passed_samples
        
        overall_pass_rate = (total_passed / total_samples * 100) if total_samples > 0 else 0.0
        
        return OverallStats(
            total_samples=total_samples,
            passed_samples=total_passed,
            pass_rate=overall_pass_rate,
            provider_stats=provider_stats
        )
    
    def _calculate_provider_source_stats(self, results: List[EvaluationResult]) -> ProviderStats:
        """Calculate stats for a specific provider/source combination"""
        
        total_samples = len(results)
        passed_samples = sum(1 for r in results if r.passed)
        pass_rate = (passed_samples / total_samples * 100) if total_samples > 0 else 0.0
        
        # Field accuracy calculation
        field_accuracy = {}
        fields_to_check = ['txn_type', 'amount', 'counterparty', 'account_ref', 'date_str', 'time_str']
        
        for field in fields_to_check:
            correct = 0
            total = 0
            
            for result in results:
                if field in result.expected_output and result.expected_output[field] is not None:
                    total += 1
                    actual_val = result.actual_output.get(field)
                    expected_val = result.expected_output[field]
                    
                    if self._values_match(actual_val, expected_val, field):
                        correct += 1
            
            field_accuracy[field] = (correct / total * 100) if total > 0 else 0.0
        
        # Direction accuracy (txn_type)
        direction_tp = 0  # True positives
        direction_fp = 0  # False positives
        direction_fn = 0  # False negatives
        
        for result in results:
            actual_type = result.actual_output.get('txn_type')
            expected_type = result.expected_output.get('txn_type')
            
            if expected_type and actual_type:
                if actual_type == expected_type:
                    direction_tp += 1
                else:
                    direction_fp += 1
                    direction_fn += 1
        
        direction_precision = (direction_tp / (direction_tp + direction_fp)) if (direction_tp + direction_fp) > 0 else 0.0
        direction_recall = (direction_tp / (direction_tp + direction_fn)) if (direction_tp + direction_fn) > 0 else 0.0
        direction_f1 = (2 * direction_precision * direction_recall / (direction_precision + direction_recall)) if (direction_precision + direction_recall) > 0 else 0.0
        
        return ProviderStats(
            total_samples=total_samples,
            passed_samples=passed_samples,
            pass_rate=pass_rate,
            field_accuracy=field_accuracy,
            direction_precision=direction_precision * 100,
            direction_recall=direction_recall * 100,
            direction_f1=direction_f1 * 100
        )
    
    def run_evaluation(self, provider_filter: Optional[str] = None, 
                      source_filter: Optional[str] = None, 
                      verbose: bool = False) -> Dict[str, Any]:
        """Run complete evaluation and return results"""
        
        # Load golden samples
        samples = self.load_golden_samples(provider_filter, source_filter)
        if not samples:
            return {"error": "No golden samples found"}
        
        print(f"Evaluating {len(samples)} samples...")
        
        # Evaluate each sample
        results = []
        for i, sample in enumerate(samples):
            if verbose and (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(samples)} samples...")
            
            result = self.evaluate_sample(sample)
            results.append(result)
        
        # Calculate statistics
        stats = self.calculate_stats(results)
        
        # Prepare output
        output = {
            "overall_stats": {
                "total_samples": stats.total_samples,
                "passed_samples": stats.passed_samples,
                "pass_rate": stats.pass_rate,
                "timestamp": json.dumps(None, default=str)  # Will be replaced with actual timestamp
            },
            "provider_stats": {},
            "detailed_results": []
        }
        
        # Add provider stats
        for provider, source_stats in stats.provider_stats.items():
            output["provider_stats"][provider] = {}
            for source, pstats in source_stats.items():
                output["provider_stats"][provider][source] = asdict(pstats)
        
        # Add detailed results if verbose
        if verbose:
            for result in results:
                if not result.passed:  # Only include failures in detailed output
                    output["detailed_results"].append({
                        "sample_id": result.sample_id,
                        "provider": result.provider,
                        "source": result.source,
                        "errors": result.errors,
                        "actual": result.actual_output,
                        "expected": result.expected_output
                    })
        
        return output

def print_results_table(stats: Dict[str, Any]):
    """Print results in a formatted table"""
    
    print(f"\n{'='*80}")
    print("PROVIDER PATTERN EVALUATION RESULTS")
    print(f"{'='*80}")
    
    # Overall stats
    overall = stats.get("overall_stats", {})
    print(f"OVERALL: {overall.get('passed_samples', 0)}/{overall.get('total_samples', 0)} samples passed   {overall.get('pass_rate', 0):.1f}%")
    print(f"{'='*80}")
    
    # Provider stats table
    print(f"\n{'Provider':<15} {'Source':<8} {'Pass%':<7} {'Dir-P':<7} {'Dir-R':<7} {'Dir-F1':<7}")
    print("-" * 64)
    
    provider_stats = stats.get("provider_stats", {})
    for provider in sorted(provider_stats.keys()):
        for source in sorted(provider_stats[provider].keys()):
            pstats = provider_stats[provider][source]
            print(f"{provider:<15} {source:<8} {pstats['pass_rate']:<6.1f}% "
                  f"{pstats['direction_precision']:<6.1f}% {pstats['direction_recall']:<6.1f}% "
                  f"{pstats['direction_f1']:<6.1f}%")

def main():
    parser = argparse.ArgumentParser(description='Evaluate provider parsing patterns')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Show detailed failure information')
    parser.add_argument('--json', action='store_true',
                       help='Output results in JSON format')
    parser.add_argument('--provider', type=str,
                       help='Filter by specific provider')
    parser.add_argument('--source', type=str,
                       help='Filter by specific source (sms, gmail, pdf)')
    parser.add_argument('--output', '-o', type=str,
                       help='Save results to file')
    
    args = parser.parse_args()
    
    # Run evaluation
    evaluator = ProviderPatternEvaluator()
    results = evaluator.run_evaluation(
        provider_filter=args.provider,
        source_filter=args.source,
        verbose=args.verbose
    )
    
    if "error" in results:
        print(f"Error: {results['error']}")
        sys.exit(1)
    
    # Output results
    if args.json:
        # JSON output for automation
        import datetime
        results["overall_stats"]["timestamp"] = datetime.datetime.now().isoformat()
        
        output_json = json.dumps(results, indent=2, default=str)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output_json)
            print(f"Results saved to {args.output}")
        else:
            print(output_json)
    else:
        # Human-readable output
        print_results_table(results)
        
        # Show detailed failures if verbose
        if args.verbose and results.get("detailed_results"):
            print(f"\n{'='*80}")
            print("DETAILED FAILURE ANALYSIS")
            print(f"{'='*80}")
            
            for failure in results["detailed_results"][:10]:  # Show first 10 failures
                print(f"\n[{failure['sample_id']}] {failure['provider'].upper()}/{failure['source'].upper()}")
                for error in failure['errors']:
                    print(f"  ❌ {error}")

def run_evaluation() -> Dict[str, Any]:
    """Function for programmatic access"""
    evaluator = ProviderPatternEvaluator()
    return evaluator.run_evaluation()

if __name__ == "__main__":
    main()