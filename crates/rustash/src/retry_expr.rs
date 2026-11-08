//! `Upstash-Retry-Delay` expression grammar, ported from `core/retry-delay.js`:
//! `+ - * / %`, parentheses, unary `+`/`-`, numbers, variable `retried`, functions
//! pow(a,b) exp sqrt abs floor ceil round min(a,b) max(a,b). Result rounded and
//! clamped to [0, 86_400_000] ms.

use crate::backoff::MAX_RETRY_DELAY_MS;
use crate::duration::ValidationError;

#[derive(Clone)]
enum Tok {
    Num(f64),
    Ident(String),
    Op(char),
}

fn tokenize(input: &str) -> Result<Vec<Tok>, ValidationError> {
    let b = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let ch = b[i];
        let c = ch as char;
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if ch.is_ascii_digit() || c == '.' {
            let rest = &input[i..];
            // /^\d+(?:\.\d+)?|^\.\d+/
            let m = if ch.is_ascii_digit() {
                let mut j = 0;
                while j < rest.len() && rest.as_bytes()[j].is_ascii_digit() {
                    j += 1;
                }
                if j < rest.len() && rest.as_bytes()[j] == b'.' {
                    let mut k = j + 1;
                    while k < rest.len() && rest.as_bytes()[k].is_ascii_digit() {
                        k += 1;
                    }
                    if k > j + 1 {
                        k
                    } else {
                        j
                    }
                } else {
                    j
                }
            } else {
                let mut j = 1;
                while j < rest.len() && rest.as_bytes()[j].is_ascii_digit() {
                    j += 1;
                }
                if j > 1 {
                    j
                } else {
                    0
                }
            };
            if m == 0 {
                return Err(ValidationError(format!("invalid number at position {}", i)));
            }
            tokens.push(Tok::Num(rest[..m].parse::<f64>().map_err(|_| {
                ValidationError(format!("invalid number at position {}", i))
            })?));
            i += m;
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == b'_' {
            let mut j = i + 1;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            tokens.push(Tok::Ident(input[i..j].to_string()));
            i = j;
            continue;
        }
        if "+-*/%(),".contains(c) {
            tokens.push(Tok::Op(c));
            i += 1;
            continue;
        }
        return Err(ValidationError(format!(
            "unexpected character \"{}\" in retry-delay expression",
            c
        )));
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Tok>,
    retried: i64,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos)
    }
    fn take_op(&mut self, value: char) -> bool {
        match self.peek() {
            Some(Tok::Op(c)) if *c == value => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }
    fn parse(&mut self) -> Result<f64, ValidationError> {
        let value = self.expr()?;
        if self.pos < self.tokens.len() {
            return Err(ValidationError(
                "unexpected trailing input in retry-delay expression".into(),
            ));
        }
        Ok(value)
    }
    fn expr(&mut self) -> Result<f64, ValidationError> {
        let mut left = self.term()?;
        loop {
            if self.take_op('+') {
                left += self.term()?;
            } else if self.take_op('-') {
                left -= self.term()?;
            } else {
                return Ok(left);
            }
        }
    }
    fn term(&mut self) -> Result<f64, ValidationError> {
        let mut left = self.unary()?;
        loop {
            if self.take_op('*') {
                left *= self.unary()?;
            } else if self.take_op('/') {
                left /= self.unary()?;
            } else if self.take_op('%') {
                left %= self.unary()?;
            } else {
                return Ok(left);
            }
        }
    }
    fn unary(&mut self) -> Result<f64, ValidationError> {
        if self.take_op('-') {
            return Ok(-self.unary()?);
        }
        if self.take_op('+') {
            return self.unary();
        }
        self.primary()
    }
    fn primary(&mut self) -> Result<f64, ValidationError> {
        let tok = match self.peek().cloned() {
            None => {
                return Err(ValidationError(
                    "unexpected end of retry-delay expression".into(),
                ))
            }
            Some(t) => t,
        };
        match tok {
            Tok::Num(v) => {
                self.pos += 1;
                Ok(v)
            }
            Tok::Op('(') => {
                self.pos += 1;
                let value = self.expr()?;
                if !self.take_op(')') {
                    return Err(ValidationError("missing closing parenthesis".into()));
                }
                Ok(value)
            }
            Tok::Op(_) => Err(ValidationError(
                "unexpected token in retry-delay expression".into(),
            )),
            Tok::Ident(name) => {
                self.pos += 1;
                if name == "retried" {
                    return Ok(self.retried as f64);
                }
                let (arity, f): (usize, fn(&[f64]) -> f64) = match name.as_str() {
                    "pow" => (2, |a| a[0].powf(a[1])),
                    "exp" => (1, |a| a[0].exp()),
                    "sqrt" => (1, |a| a[0].sqrt()),
                    "abs" => (1, |a| a[0].abs()),
                    "floor" => (1, |a| a[0].floor()),
                    "ceil" => (1, |a| a[0].ceil()),
                    "round" => (1, |a| a[0].round()),
                    "min" => (2, |a| a[0].min(a[1])),
                    "max" => (2, |a| a[0].max(a[1])),
                    _ => return Err(ValidationError(format!("unknown identifier \"{}\"", name))),
                };
                if !self.take_op('(') {
                    return Err(ValidationError(format!("expected \"(\" after {}", name)));
                }
                let mut args = vec![self.expr()?];
                while self.take_op(',') {
                    args.push(self.expr()?);
                }
                if !self.take_op(')') {
                    return Err(ValidationError(format!(
                        "missing \")\" after {} arguments",
                        name
                    )));
                }
                if args.len() != arity {
                    return Err(ValidationError(format!(
                        "{} expects {} argument(s), got {}",
                        name,
                        arity,
                        args.len()
                    )));
                }
                Ok(f(&args))
            }
        }
    }
}

/// Evaluate the expression for a given retry count; clamped to [0, 1 day] ms.
pub fn eval_retry_delay_ms(expr: &str, retried: i64) -> Result<i64, ValidationError> {
    let tokens = tokenize(expr)?;
    let result = Parser {
        tokens,
        retried,
        pos: 0,
    }
    .parse()?;
    if !result.is_finite() {
        return Err(ValidationError(
            "retry-delay expression did not evaluate to a finite number".into(),
        ));
    }
    Ok(result.round().clamp(0.0, MAX_RETRY_DELAY_MS as f64) as i64)
}

/// Ingest-time validation: parse + trial evaluation at retried=0.
pub fn validate_retry_delay_expr(expr: &str) -> Result<(), ValidationError> {
    eval_retry_delay_ms(expr, 0)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_and_functions() {
        assert_eq!(eval_retry_delay_ms("0", 3).unwrap(), 0);
        assert_eq!(eval_retry_delay_ms("retried", 4).unwrap(), 4);
        assert_eq!(eval_retry_delay_ms("retried * 1000", 2).unwrap(), 2000);
        assert_eq!(
            eval_retry_delay_ms("min(exp(2.5*retried), 86400)*1000", 0).unwrap(),
            1000
        );
        assert_eq!(eval_retry_delay_ms("(1+2)*3", 0).unwrap(), 9);
        assert_eq!(eval_retry_delay_ms("-5 + 7", 0).unwrap(), 2);
        assert_eq!(eval_retry_delay_ms("7%3", 0).unwrap(), 1);
        assert_eq!(eval_retry_delay_ms("pow(2,10)", 0).unwrap(), 1024);
        assert_eq!(eval_retry_delay_ms("sqrt(16)", 0).unwrap(), 4);
        assert_eq!(eval_retry_delay_ms("round(2.6)", 0).unwrap(), 3);
    }

    #[test]
    fn clamps_and_negative() {
        assert_eq!(eval_retry_delay_ms("-1", 0).unwrap(), 0);
        assert_eq!(
            eval_retry_delay_ms("99999999999", 0).unwrap(),
            MAX_RETRY_DELAY_MS
        );
    }

    #[test]
    fn non_finite_rejected() {
        assert!(eval_retry_delay_ms("1/0", 0).is_err());
        assert_eq!(
            eval_retry_delay_ms("1/0", 0).unwrap_err().0,
            "retry-delay expression did not evaluate to a finite number"
        );
    }

    #[test]
    fn error_strings_match_js() {
        assert_eq!(
            validate_retry_delay_expr("foo(1)").unwrap_err().0,
            "unknown identifier \"foo\""
        );
        assert_eq!(
            validate_retry_delay_expr("2 3").unwrap_err().0,
            "unexpected trailing input in retry-delay expression"
        );
        assert_eq!(
            validate_retry_delay_expr("pow(1)").unwrap_err().0,
            "pow expects 2 argument(s), got 1"
        );
        assert_eq!(
            validate_retry_delay_expr("(1+2").unwrap_err().0,
            "missing closing parenthesis"
        );
        assert_eq!(
            validate_retry_delay_expr("@").unwrap_err().0,
            "unexpected character \"@\" in retry-delay expression"
        );
    }
}
