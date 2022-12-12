import ast


class BaseParser(ast.NodeVisitor):
    def generic_visit(self, node, op=None):
        args = []
        for field, value in list(ast.iter_fields(node)):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.AST):
                        args.append(self.visit(item))
            elif isinstance(value, ast.AST):
                args.append(self.visit(value))
        args = [v for v in args if v]
        if isinstance(op, int):
            op = args[op]
            args = args[1:]

        return self._generic_visit(op, args)

    def visit_Call(self, node):
        return self.generic_visit(node, op=0)


class InOrder2PreOrderParser(BaseParser):
    def __init__(self):
        self.op_map = {
            'Add': '+',
            'Sub': '-',
            'Mult': '*',
            'Div': '/',
            'Eq': '==',
            'Gt': '>',
            'GtE': '>=',
            'Lt': '<',
            'LtE': '<=',
            'And': '&&',
            'Or': '||',
            'iif': 'if',
            'max': 'max',
            'min': 'min',
            'ln': 'ln',
            'log': 'log',
        }

    def _generic_visit(self, op, args):
        if op is None or not args:
            return ' '.join(args)
        origin_op = op
        op = self.op_map[op]

        if op == 'if':
            if len(args) != 3:
                raise Exception('%s: requires 3 args' % origin_op)
            return '(%s %s %s %s)' % (op, args[0], args[1], args[2])
        if op == 'ln':
            if len(args) != 1:
                raise Exception('%s: requires 1 arg' % origin_op)
            return '(%s %s)' % (op, args[0])

        if len(args) < 2:
            raise Exception('%s: requires at least 2 args' % origin_op)
        s = ('(%s ' % op) * (len(args) - 1)
        s += args[0]
        s += ''.join([' %s)' % v for v in args[1:]])
        return s

    def visit_Call(self, node):
        return self.generic_visit(node, op=0)

    def visit_Compare(self, node):
        return self.generic_visit(node, op=type(node.ops[0]).__name__)

    def visit_BinOp(self, node):
        return self.generic_visit(node, op=type(node.op).__name__)

    def visit_BoolOp(self, node):
        return self.generic_visit(node, op=type(node.op).__name__)

    def visit_IfExp(self, node):
        return self.generic_visit(node, op='iif')

    def visit_Name(self, node):
        return node.id

    def visit_Num(self, node):
        return str(node.n)


class GeneralFeatureParser(BaseParser):
    def _generic_visit(self, op, args):
        return sum(args, [])

    def visit_Name(self, node):
        return [node.id]


def in_order_to_pre_order(in_order_expression):
    t = ast.parse(in_order_expression)
    parser = InOrder2PreOrderParser()
    return parser.visit(t)


def get_general_features(features):
    if not isinstance(features, (list, tuple)):
        features = [features]
    general_features = []
    parser = GeneralFeatureParser()
    for feature in features:
        t = ast.parse(feature)
        general_features += parser.visit(t)
    return sorted(set(general_features))


if __name__ == '__main__':
    print('unittest test')

    assert in_order_to_pre_order('ln(d)') == '(ln d)'
    assert in_order_to_pre_order('log(a, b)+ln(d)') == '(+ (log a b) (ln d))'
    assert in_order_to_pre_order('max(a, b, c)+ln(d)') == '(+ (max (max a b) c) (ln d))'
    assert in_order_to_pre_order('iif(a > 1, b, c)') == '(if (> a 1) b c)'
    assert in_order_to_pre_order('(b if a > 1 else c)') == '(if (> a 1) b c)'
    assert in_order_to_pre_order('a + b / c * d') == '(+ a (* (/ b c) d))'
    assert in_order_to_pre_order('a and b or (c > 2) and d') == '(|| (&& a b) (&& (> c 2) d))'

    assert get_general_features(['a + b / c * d', 'a and b or (c > 2) and d']) == ['a', 'b', 'c', 'd']

    print('great, all passed')
