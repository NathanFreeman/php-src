--TEST--
Extending the PhpToken class
--EXTENSIONS--
tokenizer
--FILE--
<?php

$code = <<<'PHP'
<?PHP
FUNCTION FOO() {
    ECHO "bar";
}
PHP;

class MyPhpToken extends PhpToken {
    public int $extra = 123;

    public function getLoweredText(): string {
        return strtolower($this->text);
    }
}

foreach (MyPhpToken::tokenize($code) as $token) {
    echo $token->getLoweredText();

    if ($token->extra !== 123) {
        echo "Missing property!\n";
    }
}

?>
--EXPECT--
<?php
function foo() {
    echo "bar";
}
