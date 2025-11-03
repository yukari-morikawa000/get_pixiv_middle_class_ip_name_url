import re
import pandas as pd

def clean_ip_name(text: str) -> str:

    # 入力が文字列でない場合（例：NaNなど）は空文字を返す
    if not isinstance(text, str):
        return ""
    
    # 行末の空文字 +「-XXX-」、「〜XXX〜」の部分を削除
    text = re.sub(r'\s*[-〜][^-\s〜]+[-〜]\s*$', '', text)

    # 半角スペースを全て削除
    text = text.replace(' ', '')

    return text

# 直接実行した場合のテスト用コード
if __name__ == '__main__':
    # テスト用のデータを作成
    data = {
        'middle_class_ip_name': [
            'キャラクター名 -シリーズ名-',
            'アイテム名 〜ゲームタイトル〜',
            '普通 の 名前',
            '技名 -必殺技-',
            'Another Name',
            None # データがない場合
        ]
    }
    df = pd.DataFrame(data)

    # 整形関数を適用
    df['clean_ip_name'] = df['middle_class_ip_name'].apply(clean_ip_name)

    print("--- 整形処理のテスト実行結果 ---")
    print(df)
    
    # 期待される結果の確認
    # 0          キャラクター名
    # 1            アイテム名
    # 2              普通の名前
    # 3                 技名
    # 4         AnotherName
    # 5