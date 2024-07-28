#!/usr/bin/perl

# 入力ファイル名
$input_file = $ARGV[0];
print $input_file;
# 出力ディレクトリ
$output_dir = $ARGV[1];
# ファイルを開く

open(my $fh, '<', $input_file) or die "ファイルが開けません: $!";

# 行数カウント
my $line_count = 0;

# ファイル分割カウンタ
my $part = 0;

# バッファ
my $buffer = "";

while (<$fh>) {
    # バッファに1行追加
    $buffer .= $_;

    # 100行に達したらファイルを分割
    if ($line_count % 2000000 == 0) {
        # ファイルを開く
        open(my $out, '>', sprintf("${output_dir}/sra_accessiosn_part_%03d.txt", ++$part)) or die "ファイルが開けません: $!";

        # バッファの内容を書き込み
        print $out $buffer;

        # バッファを初期化
        $buffer = "";

        # ファイルを閉じる
        close($out);
    }

    # 行数カウント
    $line_count++;
}

# バッファの内容を書き込み
if ($buffer ne "") {
    open(my $out, '>', sprintf("${output_dir}/sra_accessiosn_part_%03d.txt", ++$part)) or die "ファイルが開けません: $!";
    print $out $buffer;
    close($out);
}

# ファイルを閉じる
close($fh);
